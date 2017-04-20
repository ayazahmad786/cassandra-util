
package com.protectwise.cassandra.db.compaction;

import com.protectwise.cassandra.retrospect.deletion.CassandraPurgedRowData;
import com.protectwise.cassandra.util.SerializerMetaDataFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class KafkaBackupSinkForDeletingCompaction implements IDeletedRecordsSink
{
	private static final Logger logger = LoggerFactory.getLogger(KafkaBackupSinkForDeletingCompaction.class);

	protected final ColumnFamilyStore cfs;
	protected final ColumnFamily columnFamily;

	protected final long keysPerSSTable;

	protected DecoratedKey currentKey;
	protected long numCells = 0;
	protected long numKeys = 0;

	private KafkaProducer<String, String> kafkaBackupRowProducer;
	private String kafkaTopicForPurgedCassandraData;

	public KafkaBackupSinkForDeletingCompaction(ColumnFamilyStore cfs, String kafkaServers, String cassandraPurgedKafkaTopic)
	{
		// TODO: Wow, this key estimate is probably grossly over-estimated...  Not sure how to get a better one here.
		this(cfs,  cfs.estimateKeys() / cfs.getLiveSSTableCount(), kafkaServers, cassandraPurgedKafkaTopic);
	}

	public KafkaBackupSinkForDeletingCompaction(ColumnFamilyStore cfs, long keyEstimate, String kafkaServers, String kafkaTopicForPurgedCassandraData)
	{
		this.cfs = cfs;
		this.keysPerSSTable = keyEstimate;

		// Right now we're just doing one sink per compacted sstable, so they'll be pre-sorted, meaning
		// we don't need to bother resorting the data.
		columnFamily = ArrayBackedSortedColumns.factory.create(cfs.keyspace.getName(), cfs.getColumnFamilyName());

		this.kafkaTopicForPurgedCassandraData = kafkaTopicForPurgedCassandraData;

		this.kafkaBackupRowProducer = getKafkaProducer(kafkaServers);
	}

	private KafkaProducer<String, String> getKafkaProducer(String kafkaServers) {
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", kafkaServers);
		kafkaProperties.setProperty("key.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.setProperty("acks", "1");
		kafkaProperties.setProperty("retries", "5");
		kafkaProperties.setProperty("batch.size", "20");

		return new KafkaProducer<String, String>(kafkaProperties);
	}

	protected void flush()
	{
		if (!columnFamily.isEmpty())
		{
			//TODO, instead of printRow give a meaningfull name as it will put the purge data in kafka
			archiveRow(this::printCellConsumer);
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
				logger.debug("primary key: {}", column.name().cql3ColumnName(columnFamily.metadata()).toString());
				return;
			}
			try {
				if (column.value() != null && column.value().array().length > 0) {
					logger.debug("column identifier:{}", column.name().cql3ColumnName(columnFamily.metadata()).toString());
				}
			} catch (Exception e) {
				logger.warn("Exception occurred while printing cell", e);
			}
		}
	}

	private void handleNonKeyCell(Cell cell, CassandraPurgedRowData purgedData) {
		if (cell != null)  {
			Cell column = cell;
			ColumnDefinition columnDefinition = columnFamily.metadata().getColumnDefinition(column.name());
			if(columnDefinition == null) {
				return;
			}
			if(columnDefinition.isPrimaryKeyColumn()) {
				purgedData.addPartitonKey(column.name().cql3ColumnName(columnFamily.metadata()).toString(),
						SerializerMetaDataFactory.getSerializerMetaData(columnFamily.metadata().getColumnDefinition(column.name()).type.getSerializer()),
						column.value(), Long.valueOf(column.timestamp()));
				return;
			} else {
				purgedData.addNonKeyCell(SerializerMetaDataFactory.getSerializableCellData(cell, columnFamily), column.name().cql3ColumnName(columnFamily.metadata()).toString(), SerializerMetaDataFactory.getSerializerMetaData(columnFamily.metadata().getColumnDefinition(column.name()).type.getSerializer()));

			}
		}
	}

	private void archiveRow(Consumer<Cell> printCell) {
		CassandraPurgedRowData cassandraPurgedData = new CassandraPurgedRowData();
		cassandraPurgedData.setKsName(columnFamily.metadata().ksName);
		cassandraPurgedData.setCfName(columnFamily.metadata().cfName);

		//retrieve partition key, clustering key value and put in the serializerMetaData
		handlePartitionKey(currentKey, columnFamily.metadata(), cassandraPurgedData);
		columnFamily.forEach(cell-> {
			handleClusteringKey(cell, columnFamily.metadata(), cassandraPurgedData);
			handleNonKeyCell(cell, cassandraPurgedData);
		});


		ObjectMapper objectMapper = new ObjectMapper();


		String key = getKafkaMessageKey(columnFamily);

		try {
			kafkaBackupRowProducer.send(new ProducerRecord<String, String>(kafkaTopicForPurgedCassandraData, key, objectMapper.writeValueAsString(cassandraPurgedData)));
		} catch (Exception e) {
			logger.warn("Exception occurred while queuing data for keyspace: {}, columnFamily: {}, partitionKey: {}, clusterKey: {}",
					cassandraPurgedData.getKsName()
					, cassandraPurgedData.getCfName()
					, cassandraPurgedData.getPartitionKeys()
					, cassandraPurgedData.getClusterKeys(), e);
		}
	}

	private void handleClusteringKey(Cell cell, CFMetaData metadata, CassandraPurgedRowData cassandraPurgedData) {
		for (ColumnDefinition def : metadata.clusteringColumns()) {
			try {
				cassandraPurgedData.addClusteringKey(ByteBufferUtil.string(def.name.bytes),
						SerializerMetaDataFactory.getSerializerMetaData(def.type.getSerializer()), cell.name().get(def.position()), null);
			}catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void handlePartitionKey(DecoratedKey currentKey, CFMetaData metadata, CassandraPurgedRowData cassandraPurgedData) {
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
		logger.info("started kafka based backup for ksName: {}, columnFamily: {}",
				columnFamily.metadata().ksName, columnFamily.metadata().cfName);
	}

	@Override
	public void close() throws IOException
	{
		if (numKeys > 0 && numCells > 0)
		{
			flush();
			logger.info("Cleanly closing backup operation for ksName: {}, columnFamily: {}, with {} keys and {} cells",
					columnFamily.metadata().ksName, columnFamily.metadata().cfName, numKeys, numCells);
		}
		else
		{
			// If deletion convicted nothing, then don't bother writing an empty backup file.
			abort();
		}

		try {
			kafkaBackupRowProducer.close();
		}catch(Exception e) {
			logger.error("Couldn't cleanly stop kafka producer for ksName: {}, columnFamily: {}",
					columnFamily.metadata().ksName, columnFamily.metadata().cfName, e);
		}
	}

	/**
	 * Abort the operation and discard any outstanding data.
	 * Only one of close() or abort() should be called.
	 */
	@Override
	public void abort()
	{
		logger.info("Aborting backup operation for ksName: {}, columnFamily: {}", columnFamily.metadata().ksName, columnFamily.metadata().cfName);
		columnFamily.clear();
	}
}
