package com.simility.cassandra.purgeddata.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.codehaus.jackson.map.ObjectMapper;
import com.protectwise.cassandra.retrospect.deletion.CassandraPurgedData;
import com.protectwise.cassandra.retrospect.deletion.SerializableColumnData;
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
		consumer.subscribe(Arrays.asList("cassandra_compaction_deleted_data_topic"));
	}
	
	public ConsumerRecords<String, String> pollRecord() {
		return consumer.poll(POLL_TIMEOUT);
	}
	
	public CassandraPurgedData getObject(String json) {
		try {
			return objectMapper.readValue(json, CassandraPurgedData.class);
		} catch (Exception e) {
			//e.printStackTrace();
			throw new RuntimeException(e);
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
					CassandraPurgedData purgedData = testConsumer.getObject(record.value());
					for(Entry<String, SerializableColumnData> entry :purgedData.getColumnSerializedValues().entrySet()) {
						System.out.println(entry.getKey() + ":" + 
											purgedData.getColumnSerializerMetaDatas().get(entry.getKey()).getSerializer().deserialize(ByteBuffer.wrap(entry.getValue().getValue())));
					}
				}catch(Exception e) {
					e.printStackTrace();
					int x = 5;
				}
			});
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
			System.out.println("Total records: " + count.get());
		}
	}
}
