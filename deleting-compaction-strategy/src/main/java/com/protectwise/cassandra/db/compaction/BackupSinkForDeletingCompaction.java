/*
 * Copyright 2016 ProtectWise, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.protectwise.cassandra.db.compaction;

import com.protectwise.cassandra.retrospect.deletion.CassandraPurgedData;
import com.protectwise.cassandra.util.PrintHelper;
import com.protectwise.cassandra.util.SerializerMetaDataFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.math3.filter.KalmanFilter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class BackupSinkForDeletingCompaction implements IDeletedRecordsSink
{
	private static final Logger logger = LoggerFactory.getLogger(BackupSinkForDeletingCompaction.class);

	protected final ColumnFamilyStore cfs;
	protected final ColumnFamily columnFamily;
	protected final File targetDirectory;
	protected final long keysPerSSTable;

	protected SSTableWriter writer;
	protected DecoratedKey currentKey;
	protected long numCells = 0;
	protected long numKeys = 0;

	private KafkaProducer<String, String> backupRowproducer;
	private String cassandraPurgedKafkaTopic;

	public BackupSinkForDeletingCompaction(ColumnFamilyStore cfs, File targetDirectory, String kafkaServers, String cassandraPurgedKafkaTopic)
	{
		// TODO: Wow, this key estimate is probably grossly over-estimated...  Not sure how to get a better one here.
		this(cfs, targetDirectory, cfs.estimateKeys() / cfs.getLiveSSTableCount(), kafkaServers, cassandraPurgedKafkaTopic);
	}

	public BackupSinkForDeletingCompaction(ColumnFamilyStore cfs, File targetDirectory, long keyEstimate, String kafkaServers, String cassandraPurgedKafkaTopic)
	{
		this.cfs = cfs;
		this.targetDirectory = targetDirectory;
		this.keysPerSSTable = keyEstimate;

		// Right now we're just doing one sink per compacted sstable, so they'll be pre-sorted, meaning
		// we don't need to bother resorting the data.
		columnFamily = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.getColumnFamilyName());

		this.cassandraPurgedKafkaTopic = cassandraPurgedKafkaTopic;

		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", kafkaServers);
		kafkaProperties.setProperty("key.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.setProperty("acks", "1");
		kafkaProperties.setProperty("retries", "5");
		kafkaProperties.setProperty("batch.size", "20");

		this.backupRowproducer = new KafkaProducer<String, String>(kafkaProperties);
	}

	protected void flush()
	{
		if (!columnFamily.isEmpty())
		{
			//TODO, instead of printRow give a meaningfull name as it will put the purge data in kafka
			nonLocalRowArchive(this::printCellConsumer);
			writer.append(currentKey, columnFamily);
			columnFamily.clear();
		}
	}

	private void printCellConsumer(Cell cell) {
		if (cell != null)  {
			Cell column = cell;
			ColumnDefinition columnDefinition = columnFamily.metadata().getColumnDefinition(column.name());
			if(columnDefinition == null) {
				return;
			}
			if(columnDefinition.isPrimaryKeyColumn()) {
				logger.info("primary key: {}", column.name().cql3ColumnName(columnFamily.metadata()).toString());
				return;
			}
			try {
				if (column.value() != null && column.value().array().length > 0) {
					logger.debug("column identifier:{}", column.name().cql3ColumnName(columnFamily.metadata()).toString());
					//logger.info("Column value: {}", columnFamily.metadata().getColumnDefinition(column.name()).type.getSerializer().deserialize(column.value()));
					//logger.info("Column serilizer: {}", columnFamily.metadata().getColumnDefinition(column.name()).type.getSerializer().getClass().getName());
				}
			} catch (Exception e) {
				logger.warn("Exception occurred while printing cell", e);
			}
		}
	}

	private void handleNonLocalArchiving(Cell cell, CassandraPurgedData purgedData) {
		if (cell != null)  {
			Cell column = cell;
			ColumnDefinition columnDefinition = columnFamily.metadata().getColumnDefinition(column.name());
			if(columnDefinition == null) {
				return;
			}
			if(columnDefinition.isPrimaryKeyColumn()) {
				logger.debug("primary key: {}", column.name().cql3ColumnName(columnFamily.metadata()).toString());
				purgedData.addPartitonKey(column.name().cql3ColumnName(columnFamily.metadata()).toString(),
						SerializerMetaDataFactory.getSerializerMetaData(columnFamily.metadata().getColumnDefinition(column.name()).type.getSerializer()),
						column.value(), Long.valueOf(column.timestamp()));
				return;
			} else {
				purgedData.addNonKeyCell(SerializerMetaDataFactory.getSerializableCellData(cell, columnFamily), column.name().cql3ColumnName(columnFamily.metadata()).toString(), SerializerMetaDataFactory.getSerializerMetaData(columnFamily.metadata().getColumnDefinition(column.name()).type.getSerializer()));

			}
		}
	}

	//TODO: move this to a different backup sink class
	private void nonLocalRowArchive(Consumer<Cell> printCell) {
		CassandraPurgedData cassandraPurgedData = new CassandraPurgedData();
		cassandraPurgedData.setKsName(columnFamily.metadata().ksName);
		cassandraPurgedData.setCfName(columnFamily.metadata().cfName);

		//retrieve partition key, clustering key value and put in the serializerMetaData
		handlePartitionKey(currentKey, columnFamily.metadata(), cassandraPurgedData);
		columnFamily.forEach(cell-> {
			//logger.info("Clustering keys: {}", PrintHelper.printClusteringKeys(cell, columnFamily.metadata()));
			handleClusteringKey(cell, columnFamily.metadata(), cassandraPurgedData);
			handleNonLocalArchiving(cell, cassandraPurgedData);
			//printCell.accept(cell);
		});


		ObjectMapper objectMapper = new ObjectMapper();


		String key = getKafkaMessageKey(columnFamily);

		try {
			backupRowproducer.send(new ProducerRecord<String, String>(cassandraPurgedKafkaTopic, key, objectMapper.writeValueAsString(cassandraPurgedData)));
		} catch (Exception e) {
			logger.warn("Exception occurred while queuing data", e);
		}
	}

	private void handleClusteringKey(Cell cell, CFMetaData metadata, CassandraPurgedData cassandraPurgedData) {
		for (ColumnDefinition def : metadata.clusteringColumns()) {
			try {
				cassandraPurgedData.addClusteringKey(ByteBufferUtil.string(def.name.bytes),
						SerializerMetaDataFactory.getSerializerMetaData(def.type.getSerializer()), cell.name().get(def.position()), null);
			}catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void handlePartitionKey(DecoratedKey currentKey, CFMetaData metadata, CassandraPurgedData cassandraPurgedData) {
		ByteBuffer[] keyParts;
		AbstractType<?> validator = metadata.getKeyValidator();
		if (validator instanceof CompositeType) {
			keyParts = ((CompositeType) validator).split(currentKey.getKey());
		} else {
			keyParts = new ByteBuffer[]{
					currentKey.getKey()
			};
		}
		List<ColumnDefinition> pkc = metadata.partitionKeyColumns();
		for (ColumnDefinition def : pkc) {
			try {
				cassandraPurgedData.addPartitonKey(ByteBufferUtil.string(def.name.bytes),
						SerializerMetaDataFactory.getSerializerMetaData(def.type.getSerializer()), keyParts[def.position()], null);
			}catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private String getKafkaMessageKey(ColumnFamily columnFamily) {
		return String.join("::", columnFamily.metadata().ksName , columnFamily.metadata().cfName);
	}

	@Override
	public void accept(OnDiskAtomIterator partition)
	{
		flush();
		currentKey = partition.getKey();
		numKeys++;
		// Write through the entire partition.
		while (partition.hasNext())
		{			
			OnDiskAtom cell  = partition.next();
			accept(partition.getKey(), cell);
		}
	}

	@Override
	public void accept(DecoratedKey key, OnDiskAtom cell)
	{
		if (currentKey != key)
		{
			flush();
			numKeys++;
			currentKey = key;
		}

		numCells++;		
		columnFamily.addAtom(cell);
	}

	@Override
	public void begin()
	{
		writer = new SSTableWriter(
				cfs.getTempSSTablePath(targetDirectory),
				keysPerSSTable,
				ActiveRepairService.UNREPAIRED_SSTABLE,
				cfs.metadata,
				cfs.partitioner,
				new MetadataCollector(cfs.metadata.comparator)
		);
		logger.info("Opening backup writer for {}", writer.getFilename());
	}

	@Override
	public void close() throws IOException
	{
		if (numKeys > 0 && numCells > 0)
		{
			flush();
			logger.info("Cleanly closing backup operation for {} with {} keys and {} cells", writer.getFilename(), numKeys, numCells);
			writer.close();
		}
		else
		{
			// If deletion convicted nothing, then don't bother writing an empty backup file.
			abort();
		}

		try {
			backupRowproducer.close();
		}catch(Exception e) {
			logger.error("Couldn't cleanly stop kafka producer exception ", e);
		}
	}

	/**
	 * Abort the operation and discard any outstanding data.
	 * Only one of close() or abort() should be called.
	 */
	@Override
	public void abort()
	{
		logger.info("Aborting backup operation for {}", writer.getFilename());
		columnFamily.clear();
		writer.abort();
	}
}
