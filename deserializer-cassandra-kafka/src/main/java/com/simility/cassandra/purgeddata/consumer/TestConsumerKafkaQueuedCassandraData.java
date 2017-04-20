package com.simility.cassandra.purgeddata.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.protectwise.cassandra.db.compaction.DeletingCompactionStrategyOptions;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.codehaus.jackson.map.ObjectMapper;
import com.protectwise.cassandra.retrospect.deletion.CassandraPurgedRowData;
import com.protectwise.cassandra.retrospect.deletion.SerializableCellData;
import com.protectwise.cassandra.util.SerializerMetaData;

public class TestConsumerKafkaQueuedCassandraData {

	private static Properties kafkaConsumerProperties;
	private static final long POLL_TIMEOUT = 1000;
	private ObjectMapper objectMapper = new ObjectMapper();
	
	static {
		kafkaConsumerProperties = new Properties();
		kafkaConsumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		kafkaConsumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerProperties.setProperty("enable.auto.commit", "true");
		kafkaConsumerProperties.setProperty("group.id", "cassandra_purged_data_consumer");
		kafkaConsumerProperties.setProperty("auto.offset.reset", "earliest");
		kafkaConsumerProperties.setProperty("fetch.min.bytes", "102400");
	}
	
	private static Consumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaConsumerProperties);
	
	
	public void subscribe() {
		consumer.subscribe(Arrays.asList("dcs_kafka_purged_data_default_topic"));
	}
	
	public ConsumerRecords<String, String> pollRecord() {
		return consumer.poll(POLL_TIMEOUT);
	}
	
	public CassandraPurgedRowData getObject(String json) {
		try {
			return objectMapper.readValue(json, CassandraPurgedRowData.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void printDeserializedValues(CassandraPurgedRowData purgedData) {
		for(Entry<String, SerializerMetaData> columnSerializedMetaDataEntry : purgedData.getColumnSerializerMetaDatas().entrySet()) {
			for(Entry<String, SerializableCellData> entry :purgedData.getCellSerializedValues().entrySet()) {
				int index = entry.getValue().getCellId().indexOf(columnSerializedMetaDataEntry.getKey());
				if(index == 0) {
					System.out.println(entry.getKey() + "=" + columnSerializedMetaDataEntry.getValue().getCellValue(entry.getValue().getValue()));
				}
			}
		}
	}
	public static void main(String []args) {
		TestConsumerKafkaQueuedCassandraData testConsumer = new TestConsumerKafkaQueuedCassandraData();
		testConsumer.subscribe();
		
		AtomicLong count = new AtomicLong(0);
		while(true) {
			ConsumerRecords<String, String> records = testConsumer.pollRecord();
			System.out.println("total records polled: " + records.count());
			records.forEach(record->{
				//System.out.println("key: " + record.key());
				try {
					System.out.println(record.value());
					count.incrementAndGet();
					CassandraPurgedRowData purgedData = testConsumer.getObject(record.value());
					//printCellSerializedContent(purgedData);
					printDeserializedValues(purgedData);
				}catch(Exception e) {
					e.printStackTrace();
					int x = 5;
				}
			});
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Total records: " + count.get());
		}
	}

	private static void printCellSerializedContent(CassandraPurgedRowData purgedData) {
		for(Entry<String, SerializableCellData> entry :purgedData.getCellSerializedValues().entrySet()) {
            System.out.println(entry.getKey() + "= " +
                                entry.getValue().getValue());
        }
	}
}
