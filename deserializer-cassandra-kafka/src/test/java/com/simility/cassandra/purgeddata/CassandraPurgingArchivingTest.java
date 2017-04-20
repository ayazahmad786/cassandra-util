package com.simility.cassandra.purgeddata;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.protectwise.cassandra.retrospect.deletion.CassandraPurgedRowData;
import com.protectwise.cassandra.retrospect.deletion.SerializableCellData;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ayaz on 15/4/17.
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraPurgingArchivingTest {
    private static final Logger logger = LoggerFactory.getLogger(CassandraPurgingArchivingTest.class);
    private static CassandraConfig cassandraConfig = new CassandraConfig();
    private static Properties kafkaConsumerProperties = new Properties();

    static {
        kafkaConsumerProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaConsumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProperties.setProperty("enable.auto.commit", "true");
        kafkaConsumerProperties.setProperty("group.id", "cassandra_purged_data_ftest_consumer");
        kafkaConsumerProperties.setProperty("auto.offset.reset", "earliest");
        kafkaConsumerProperties.setProperty("fetch.min.bytes", "102400");
        kafkaConsumerProperties.put("auto.commit.interval.ms", "500");
    }
    private Cluster cluster;
    private KafkaConsumer<String, String> kafkaConsumer;
    private Session session;

    @Before
    public void setup() {
        cluster = CassandraPurgingArchivingTestHelper.setUpCassandraCluster(cassandraConfig);
        kafkaConsumer = CassandraPurgingArchivingTestHelper.setupKafkaConsumer(kafkaConsumerProperties);
        session = cluster.connect();
    }

    @After
    public void tearDown() {

        //delete keyspace and table
        try {
            CassandraPurgingArchivingTestHelper.cleanUpCassandra(session);
        }catch (Exception e) {
            logger.warn("couldn't clean up cassandra  test keysapce and table", e);
        }

        try {
            cluster.close();
        }catch(Exception e) {
            logger.warn("couldn't close cluster successfully", e);
        }

        try {
            kafkaConsumer.close();
        }catch (Exception e) {
            logger.warn("Couldn't close kafka consumer successfully", e);
        }
    }

    @Test
    public void testCassandraPrimitiveSerializationDeserialization() throws IOException {
        Assert.assertNotNull(cluster);
        Assert.assertNotNull(kafkaConsumer);
        Assert.assertNotNull(session);

        CassandraPurgingArchivingTestHelper.setUpCassandraForPrimitiveDataTypes(session, "localhost:9092");

        CassandraPurgingArchivingTestHelper.insertTestDataForPrimitives(session);

        CassandraPurgingArchivingTestHelper.issueFlushAllAndCompaction();

        CassandraPurgingArchivingTestHelper.subscribeConsumer(kafkaConsumer);


        ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);


        Assert.assertNotNull(records);
        Assert.assertFalse(records.isEmpty());
        Assert.assertTrue(records.count()==1);
        ObjectMapper objectMapper = new ObjectMapper();

        String message = records.iterator().next().value();
        CassandraPurgedRowData purgedData = objectMapper.readValue(records.iterator().next().value(), CassandraPurgedRowData.class);
        for(Map.Entry<String, SerializableCellData> cellDataEntry : purgedData.getCellSerializedValues().entrySet()) {
            SerializableCellData cellData = cellDataEntry.getValue();
            Assert.assertTrue(cellData.getCellIdComponentPositions() == null || cellData.getCellIdComponentPositions().isEmpty());

            //for primitive type cellId is columnName
            String columnName = cellData.getCellId();

            Object deserializedValue = purgedData.getColumnSerializerMetaDatas().get(columnName).getCellValue(cellData.getValue());

            logger.info("cell key: {}, value is: {}", cellDataEntry.getKey(), deserializedValue.toString());
            Assert.assertNotNull(deserializedValue);
        }
        logger.info("message is: {}", message);
        Assert.assertNotNull(purgedData);
    }
}
