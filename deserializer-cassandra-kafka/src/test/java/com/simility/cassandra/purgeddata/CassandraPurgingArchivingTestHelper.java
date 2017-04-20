package com.simility.cassandra.purgeddata;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.*;

import static com.datastax.driver.core.DataType.Name.*;

/**
 * Created by ayaz on 15/4/17.
 */
public class CassandraPurgingArchivingTestHelper {
    private static final String CASSANDRA_SERVER_PROPERTY = "cassandra.host";

    private static final String KAFKA_SERVER_PROPERTY = "kafka.host";

    private static final String DEFAULT_CASSANDRA_SERVER = "127.0.0.1";

    private static final String DEFAULT_CASSANDRA_PORT = "9042";

    private static final String DEFAULT_KAFKA_SERVER = "127.0.0.1:9092";

    private static final String TEST_KEYSPACE = "testkeyspace";

    private static  final String TEST_TABLE = "testtable";

    private static final String TEST_DEL_RULE_TTL_TABLE = "deletion_rules_ttl";

    private static final int DEFAULT_DEL_RULE_TTL_SECS = 0;

    private static final String DEFAULT_CASSANDRA_JMX_PORT = "7199";

    private static final String DEFAULT_CASSANDRA_PURGED_KAFKA_TOPIC = "test_cassandra_purged_data";

    private static final Map<String, DataType> TEST_TABLE_PARTITON_KEYS;
    static {
        Map <String, DataType> aMap = new LinkedHashMap<>();
        aMap.put("partiton_key_text1", DataType.text());
        aMap.put("partition_key_text2", DataType.text());

        TEST_TABLE_PARTITON_KEYS = Collections.unmodifiableMap(aMap);
    }

    private static final Map<String, DataType> TEST_TABLE_CLUSTER_KEYS;
    static {
        Map <String, DataType> aMap = new LinkedHashMap<>();
        aMap.put("cluster_key1", DataType.text());
        aMap.put("cluster_key2", DataType.cint());

        TEST_TABLE_CLUSTER_KEYS = Collections.unmodifiableMap(aMap);
    }

    private static final Map<String, DataType> TEST_TABLE_NON_KEY_PRIMITIVE_COLS;
    static {
        Map <String, DataType> aMap = new LinkedHashMap<>();
        aMap.put("non_key_blob", DataType.blob());
        aMap.put("non_key_text", DataType.text());
        aMap.put("non_key_ascii", DataType.ascii());
        aMap.put("non_key_bool", DataType.cboolean());
        aMap.put("non_key_int", DataType.cint());
        aMap.put("non_key_bigint", DataType.bigint());
        aMap.put("non_key_float", DataType.cfloat());
        aMap.put("non_key_double", DataType.cdouble());
        aMap.put("non_key_decimal", DataType.decimal());
        aMap.put("non_key_uuid", DataType.uuid());
        aMap.put("non_key_timestamp", DataType.timestamp());

        TEST_TABLE_NON_KEY_PRIMITIVE_COLS = Collections.unmodifiableMap(aMap);
    }

    private static final Map<String, DataType> TEST_DEL_TTL_RULE_TABLE_PARTITON_KEYS;
    static {
        Map <String, DataType> aMap = new LinkedHashMap<>();
        aMap.put("ks", DataType.text());
        TEST_DEL_TTL_RULE_TABLE_PARTITON_KEYS = Collections.unmodifiableMap(aMap);
    }

    private static final Map<String, DataType> TEST_DEL_TTL_RULE_TABLE_CLUSTER_KEYS;
    static {
        Map <String, DataType> aMap = new LinkedHashMap<>();
        aMap.put("tbl", DataType.text());
        TEST_DEL_TTL_RULE_TABLE_CLUSTER_KEYS = Collections.unmodifiableMap(aMap);
    }

    private static final Map<String, DataType> TEST_DEL_TTL_RULE_TABLE_NON_KEY_COLS;
    static {
        Map <String, DataType> aMap = new LinkedHashMap<>();
        aMap.put("column", DataType.text());
        aMap.put("range_lower",  DataType.text());
        aMap.put("range_upper", DataType.text());
        aMap.put("rulename", DataType.text());
        aMap.put("ttl", DataType.bigint());

        TEST_DEL_TTL_RULE_TABLE_NON_KEY_COLS = Collections.unmodifiableMap(aMap);
    }

    private static final Integer [] ASCII_VALID_CHAR_CODE;
    static {
        List<Integer> asciiChars = new ArrayList<>();
        for(int charCode = 65; charCode <= 90; charCode++) {
            asciiChars.add(charCode);
        }
        ASCII_VALID_CHAR_CODE = asciiChars.toArray(new Integer[asciiChars.size()]);
    }

    private static final Map<DataType.Name, Random> RANDOM_GENERATORS;
    static {
        Map<DataType.Name, Random> aRandGenerators = new HashMap<>();
        Random randAscii = new Random(3);
        aRandGenerators.put(ASCII, randAscii);
        aRandGenerators.put(TEXT, randAscii);

        Random randByte = new Random(5);
        aRandGenerators.put(BLOB, randByte);

        Random randInt = new Random(7);

        Random randBoolean = new Random(11);

        aRandGenerators.put(BOOLEAN, randBoolean);

        aRandGenerators.put(INT, randInt);
        aRandGenerators.put(BIGINT, randInt);

        Random randDouble = new Random(13);

        aRandGenerators.put(FLOAT, randDouble);
        aRandGenerators.put(DOUBLE, randDouble);
        aRandGenerators.put(DECIMAL, randDouble);

        RANDOM_GENERATORS = Collections.unmodifiableMap(aRandGenerators);

    }
    /**
     * Get a instance of cassandra cluster
     */
    public static Cluster setUpCassandraCluster(CassandraConfig cassandraConfig) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, cassandraConfig.getLocalCoreConnections());
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, cassandraConfig.getLocalMaxConnections());
        poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, cassandraConfig.getRemoteCoreConnections());
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, cassandraConfig.getRemoteMaxConnections());

        final int maxRetryCountWithWait = 5;
        final int waitInMillis = 1000;

        RetryPolicy retryPolicy = new RetryPolicy() {
            public RetryDecision onReadTimeout(Statement stmt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
                return this.retryOrThrowWithWait(cl, nbRetry);
            }

            public RetryDecision onUnavailable(Statement stmt, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
                return this.retryOrThrowWithWait(cl, nbRetry);
            }

            @Override
            public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
                return null;
            }

            @Override
            public void init(Cluster cluster) {

            }

            @Override
            public void close() {

            }

            public RetryDecision onWriteTimeout(Statement stmt, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
                return this.retryOrThrowWithWait(cl, nbRetry);
            }

            /**
             * Retry after 'waitInMillis'(1 Second) wait.
             * @param cl
             * @param nbRetry
             * @return
             */
            private RetryDecision retryOrThrowWithWait(ConsistencyLevel cl, int nbRetry) {
                if (nbRetry <= maxRetryCountWithWait) {
                    if (nbRetry > 0) { // Execute first query retry without wait.
                        try {
                            // Wait for one second in Subsequent query retries.
                            Thread.sleep(waitInMillis);
                        } catch (InterruptedException e) {
                        }
                    }
                    return RetryDecision.retry(cl);
                } else {
                    return RetryDecision.rethrow();
                }
            }
        };

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(cassandraConfig.getConnectTimeoutMillis());
        socketOptions.setReadTimeoutMillis(cassandraConfig.getReadTimeoutMillis());

        QueryOptions queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(cassandraConfig.getConsistency()));
        Cluster.Builder builder = Cluster.builder()
                .withQueryOptions(queryOptions)
                .addContactPoint(cassandraConfig.getDatabaseURL())
                .withPort(cassandraConfig.getPort())
                .withClusterName(cassandraConfig.getClusterName())
                .withPoolingOptions(poolingOptions)
                .withRetryPolicy(retryPolicy)
                .withSocketOptions(socketOptions);

        if(cassandraConfig.getUsername() != null
                && cassandraConfig.getUsername().length() > 0
                && cassandraConfig.getPassword() != null
                && cassandraConfig.getPassword().length() > 0) {
            builder.withCredentials(cassandraConfig.getUsername(), cassandraConfig.getPassword());
        }
        return builder.build();

    }

    /**
     * Get a instance of kafka consumer
     */
    public static KafkaConsumer<String, String> setupKafkaConsumer(Properties kafkaConsumerProperties) {
        return new KafkaConsumer<String, String>(kafkaConsumerProperties);
    }

    public static boolean createKeySpace(Session session, String keyspace) {
            ResultSet resultSet = session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication "
                    + "= {'class':'SimpleStrategy', 'replication_factor':1};");

            return resultSet.wasApplied();
    }

    public static void cleanUpCassandra(Session session) {
        session.execute("DROP keyspace " + TEST_KEYSPACE);
    }

    public static void setUpCassandraForPrimitiveDataTypes(Session session, String kafkaBootstrapServers) {
        //create a test keyspace

        if(!createKeySpace(session, TEST_KEYSPACE)) {
            throw new RuntimeException("Couldn't create test keyspace " + TEST_KEYSPACE);
        }
        //create test table with all primitive data types
        //int, float, double text, ascii, uuid, timestamp, blob, boolean, bigint

        Create createTable = getCreateTable(TEST_KEYSPACE, TEST_TABLE,
                TEST_TABLE_PARTITON_KEYS,
                TEST_TABLE_CLUSTER_KEYS,
                TEST_TABLE_NON_KEY_PRIMITIVE_COLS);

        //use deletion compaction strategy compaction
        String query = createTable.withOptions().gcGraceSeconds(1).buildInternal();
        session.execute(query);



        String fullyQualifiedTableName = TEST_KEYSPACE + "." + TEST_TABLE;
        String fullyQualifiedDeletionRuleTableName = TEST_KEYSPACE + "." + TEST_DEL_RULE_TTL_TABLE;

        //create deletion  rule ttl table
        Create createDelTTLRuleTable = getCreateTable(TEST_KEYSPACE, TEST_DEL_RULE_TTL_TABLE,
                TEST_DEL_TTL_RULE_TABLE_PARTITON_KEYS,
                TEST_DEL_TTL_RULE_TABLE_CLUSTER_KEYS,
                TEST_DEL_TTL_RULE_TABLE_NON_KEY_COLS);

        session.execute(createDelTTLRuleTable.buildInternal());

        //alter table for enabling deletion compaction strategy options
        session.execute("ALTER TABLE " + fullyQualifiedTableName+ " with compaction = " + "{ 'max_threshold': '3', 'min_threshold': '2', 'rules_select_statement': 'SELECT rulename, column, range_lower, range_upper, ttl FROM " + fullyQualifiedDeletionRuleTableName +  " WHERE ks=''" + TEST_KEYSPACE + "'' AND tbl=''" + TEST_TABLE + "''', 'dcs_kafka_servers':'" +  kafkaBootstrapServers + "', 'dcs_kafka_purged_data_topic':'" +  DEFAULT_CASSANDRA_PURGED_KAFKA_TOPIC + "', 'dcs_convictor': 'com.protectwise.cassandra.retrospect.deletion.RuleBasedLateTTLConvictor', 'dcs_underlying_compactor': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy'}");
    }

    private static Create getCreateTable(String keyspace, String tableName,
                                         Map<String, DataType> partitionKeys,
                                         Map<String, DataType> clusterKeys,
                                         Map<String, DataType> nonKeyCols) {
        Create createTable = SchemaBuilder.createTable(keyspace, tableName);
        createTable.ifNotExists();
        for (Map.Entry<String, DataType> key : partitionKeys.entrySet()) {
            createTable.addPartitionKey(key.getKey(), key.getValue());
        }

        for (Map.Entry<String, DataType> key : clusterKeys.entrySet()) {
            createTable.addClusteringColumn(key.getKey(), key.getValue());
        }

        for (Map.Entry<String, DataType> entry : nonKeyCols.entrySet()) {
            if (!partitionKeys.containsKey(entry.getKey())
                    && !clusterKeys.containsKey(entry.getKey())) {
                createTable.addColumn(entry.getKey(), entry.getValue());
            }
        }
        return createTable;
    }

    public static void insertTestDataForPrimitives(Session session) {
        //insert into ttl rule table for partition key

        insertIntoTtlRule(session);

        //insert into main table
        Insert insertStatement = QueryBuilder.insertInto(TEST_KEYSPACE, TEST_TABLE);

        //partition keys
        for(Map.Entry<String, DataType> partitionKeyPart: TEST_TABLE_PARTITON_KEYS.entrySet()) {
            insertStatement = insertStatement.value(partitionKeyPart.getKey(), getDataOfPrimitiveType(partitionKeyPart.getValue()));
        }
        //cluster key
        for(Map.Entry<String, DataType> clusterKeyPart: TEST_TABLE_CLUSTER_KEYS.entrySet()) {
            insertStatement = insertStatement.value(clusterKeyPart.getKey(), getDataOfPrimitiveType(clusterKeyPart.getValue()));
        }
        //non key column
        for(Map.Entry<String, DataType> nonKeyColumnEntry : TEST_TABLE_NON_KEY_PRIMITIVE_COLS.entrySet()) {
            insertStatement = insertStatement.value(nonKeyColumnEntry.getKey(), getDataOfPrimitiveType(nonKeyColumnEntry.getValue()));
        }

        session.execute(insertStatement);
    }

    public static Object getDataOfPrimitiveType(DataType dataType) {
        switch(dataType.getName()) {
            case ASCII:
                return getRandomASCII(20, RANDOM_GENERATORS.get(ASCII));
            case TEXT:
                return getRandomUTF8(20, RANDOM_GENERATORS.get(TEXT));
            case BOOLEAN:
                return RANDOM_GENERATORS.get(BOOLEAN).nextBoolean();
            case BLOB:
                return getRandomBlob(100, RANDOM_GENERATORS.get(BLOB));
            case INT:
                return RANDOM_GENERATORS.get(INT).nextInt();
            case BIGINT:
                return RANDOM_GENERATORS.get(BIGINT).nextLong();
            case FLOAT:
                return RANDOM_GENERATORS.get(FLOAT).nextFloat();
            case DOUBLE:
                return RANDOM_GENERATORS.get(DOUBLE).nextDouble();
            case DECIMAL:
                return RANDOM_GENERATORS.get(DOUBLE).nextDouble();
            case UUID:
                return UUIDs.random();
            case TIMESTAMP:
                return Date.from(Clock.systemUTC().instant());
            case TIMEUUID:
                return UUIDs.timeBased();
            default:
                throw new RuntimeException("Primitive data type " + dataType.getName() + " not supported");
        }
    }

    private static ByteBuffer getRandomBlob(int length, Random rand) {
        byte []blobByte = new byte[length];
        for(int curLen = 0; curLen < length; curLen++) {
            blobByte[curLen] = (byte)(rand.nextInt(Byte.MAX_VALUE - Byte.MIN_VALUE) + Byte.MIN_VALUE);
        }
        return ByteBuffer.wrap(blobByte);
    }

    private static String getRandomUTF8(int length, Random random) {
        StringBuilder utf8Str = new StringBuilder();
        for(int curLen = 0; curLen < length; curLen++) {
            utf8Str.append(Character.toString((char)random.nextInt(65536)));
        }

        return utf8Str.toString();
    }

    private static String getRandomASCII(int length, Random random) {
        StringBuilder asciiStr = new StringBuilder();
        for(int curLen = 0; curLen < length; curLen++) {
            asciiStr.append(Character.toString((char)random.nextInt(91)));
        }

        return asciiStr.toString();
    }

    private static void insertIntoTtlRule(Session session) {
        Insert insertStatement = QueryBuilder.insertInto(TEST_KEYSPACE, TEST_DEL_RULE_TTL_TABLE)
                .value("ks", TEST_KEYSPACE)
                .value("tbl", TEST_TABLE)
                .value("column", TEST_TABLE_PARTITON_KEYS.entrySet().iterator().next().getKey())
                .value("rulename",  "test_del_compaction_primitive")
                .value("ttl", DEFAULT_DEL_RULE_TTL_SECS);

        session.execute(insertStatement);
    }

    /**
     * Sets up cassandra for testing of purging and archiving for list collection type
     * with all subtypes as primitive types
     */
    public void setUpCassandraForListCollectionType() {
        //create a test keyspace

        //create test table with all primitive data types

        //create fields list<text>, list<int>, list<double>,
        // list<float>, list<ascii>, list<timestamp>, list<uuid>, list<blob>
    }

    /**
     * Sets up cassandra for testing of purging and archiving for set collection type
     * with all subtypes as primitive types
     */
    public void setUpCassandraForSetCollectionType() {
        //create a test keyspace

        //create test table with all primitive data types

        //create fields set<text>, set<int>, set<double>,
        // set<float>, set<ascii>, set<timestamp>, set<uuid>, set<blob>
    }

    /**
     * Sets up cassandra for testing of purging and archiving for map collection type
     * with all subtypes as primitive types
     */
    public void setUpCassandraForMapCollectionType() {
        //create a test keyspace

        //create test table with all primitive data types

        //create fields map<text, text>, map<text, int>, map<text, double>,
        // map<text, float>, map<text, ascii>, map<text, timestamp>, map<text, uuid>, map<text, blob>
    }

    //TODO use JMX for calling nodetool flush and nodetool compact
/*    public static void issueFlushAllAndCompaction() {
        JMXServiceURL url;
        try {
            url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi:/" + ":" + DEFAULT_CASSANDRA_JMX_PORT+ "/jmxrmi");
        }catch (Exception e) {
            throw new RuntimeException(e);
        }

        try(JMXConnector jmxc = JMXConnectorFactory.connect(url, null)) {
            MBeanServerConnection mbsc =
                    jmxc.getMBeanServerConnection();
            //create a storageservicembean
            ObjectName mbeanName = new ObjectName("org.apache.cassandra.db:type=StorageService");
            StorageServiceMBean mbeanProxy = JMX.newMBeanProxy(mbsc, mbeanName, StorageServiceMBean.class, true);

            //call flush on test keyspace and testtable
            mbeanProxy.forceKeyspaceFlush(TEST_KEYSPACE, TEST_TABLE);

            mbeanProxy.forceKeyspaceCompaction(TEST_KEYSPACE, TEST_TABLE);

        }catch(Exception e) {
            throw new RuntimeException(e);
        }
    }*/

    public static void issueFlushAllAndCompaction() {
        try {
            String nodeToolFlushCmd = "nodetool flush " + TEST_KEYSPACE + " " + TEST_TABLE;
            String nodeToolCompactCmd = "nodetool compact " + TEST_KEYSPACE + " " + TEST_TABLE;

            executeShellCommand(nodeToolFlushCmd);
            executeShellCommand(nodeToolCompactCmd);
        }catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String executeShellCommand(String command) {
        try {
            Runtime run = Runtime.getRuntime();
            Process pr = run.exec(command);
            pr.waitFor();
            String errorMessage = getInputStreamContent(pr.getErrorStream());
            String output = "";
            if(errorMessage == null || errorMessage.isEmpty()) {
                output = getInputStreamContent(pr.getInputStream());
                System.out.println("output: " + output + " status: " + pr.exitValue());
                pr.destroyForcibly();
            } else {
                pr.destroyForcibly();
                throw new RuntimeException(errorMessage);
            }
            return output;
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getInputStreamContent(InputStream iStream) throws IOException {
        try(BufferedReader buf = new BufferedReader(new InputStreamReader(iStream))) {
            StringBuilder cmdoutputBuilder = new StringBuilder();
            String line = "";
            while ((line=buf.readLine())!=null) {
                cmdoutputBuilder.append(line);
            }
            return cmdoutputBuilder.toString();
        }
    }

    public static void subscribeConsumer(KafkaConsumer consumer) {
        consumer.subscribe(Arrays.asList(DEFAULT_CASSANDRA_PURGED_KAFKA_TOPIC));
    }
}
