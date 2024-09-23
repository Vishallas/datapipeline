package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;


import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import org.apache.avro.data.Json;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;


public class DataProcessorUtil {
    private static final Logger log = LoggerFactory.getLogger(DataProcessorUtil.class);
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final String TOPIC = "hadoop_data_1";
    private final int PARTITION = 0;
    private PreparedStatement insertIntoAppendable;
    private PreparedStatement updateInsertable;
    private PreparedStatement getLastDataOfUser;
    private PreparedStatement newUserInsertable;
    private long kafkaOffset = -1L;

    private class Session{
        private JSONArray metas;
//        private List<Meta> metas;
        private long offset;
        private Instant firstEventTime;

        Session(String batch) throws ParseException {
            JSONObject jsonObject = new JSONObject(new JSONTokener(batch));

            setOffset(Long.parseLong(jsonObject.get("offset").toString()));
            setFirstEventTime(jsonObject.get("first_event_time").toString());

            metas = (JSONArray)jsonObject.get("events");


        }
        public JSONArray getMeta(){
            return metas;
        }
        public ByteBuffer getEventsBlob() {
            return ByteBuffer.wrap(metas.toString().getBytes());
        }
        private void setOffset(long offset) {
            this.offset = offset;
        }
        private Instant getFirstEventTime() {
            return firstEventTime;
        }

        private void setFirstEventTime(String firstEventTime) throws ParseException {
            this.firstEventTime = sdf1.parse(firstEventTime).toInstant();
        }
    }

    private KafkaConsumer<String, String> initializeKafka(String BOOTSTRAP_SERVERS, String GROUP_ID){

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "Client");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        return new KafkaConsumer<>(properties);
    }

    private void prepareStatements(CqlSession cqlSession){
        insertIntoAppendable = cqlSession.prepare(
                QueryBuilder.insertInto("appendable")
                        .value("uuid", QueryBuilder.bindMarker())
                        .value("visit_no", QueryBuilder.bindMarker())
                        .value("meta", QueryBuilder.bindMarker())
                        .build());
        updateInsertable = cqlSession.prepare(
                QueryBuilder.update("insertable")
                        .set(Assignment.setColumn("visit_count", QueryBuilder.bindMarker("last_visit"))
                        , Assignment.setColumn("last_visit", QueryBuilder.bindMarker("last_visit")))
                        .whereColumn("uuid").isEqualTo(QueryBuilder.bindMarker("uuid"))
                        .build());
        getLastDataOfUser = cqlSession.prepare(
                QueryBuilder.selectFrom("insertable")
                        .columns("visit_count", "last_visit")
                        .whereColumn("uuid")
                        .isEqualTo(QueryBuilder.bindMarker("uuid"))
                        .build());
        newUserInsertable = cqlSession.prepare(
                QueryBuilder.insertInto("insertable")
                        .value("uuid", QueryBuilder.bindMarker())
                        .value("visit_count", QueryBuilder.bindMarker())
                        .value("first_visit", QueryBuilder.bindMarker())
                        .value("last_visit", QueryBuilder.bindMarker())
                        .build());
    }




    private int processUser(Jedis jedis, Writer orcWriter, CqlSession cqlSession,
                            String key, VectorizedRowBatch orcBatch, BytesColumnVector orcUuid,
                            LongColumnVector orcVisit_no, BytesColumnVector orcMeta) throws ParseException, IOException {
        final int BATCH_SIZE = 5;
        long totalSessions = jedis.llen(key);
        String uuid = key.substring(5);
        log.info("Starting session processing for {} with {} events.", uuid, totalSessions);

        // Initial user
        long localVisitCount = 0L;

        Instant localLastVisit = null;
        Instant localFirstVisit = null;

        Boolean firstTime = true;

        int userCount = 0;

        Row rs = cqlSession.execute(getLastDataOfUser.bind(uuid)).one();

        if (rs != null){
//            log.info(" [+] New User {}.", uuid);
            localLastVisit = rs.getInstant("last_visit");
            localVisitCount = rs.getLong("visit_count");
            firstTime = false;
        }

        BatchStatementBuilder batchStatementBuilder = BatchStatement.builder(BatchType.LOGGED);

        for(long i = 0;i<totalSessions;i++) {
            List<String> jsonSessions = jedis.lrange(key, 0, 0);

            for (String jsonSession : jsonSessions) {
                Session session = new Session(jsonSession);
                if (localLastVisit == null){
                    localLastVisit = session.firstEventTime;
                    localFirstVisit = localLastVisit;
                }else if(Duration.between(session.firstEventTime,localLastVisit).toMillis()<0L){
                    localVisitCount++;
                    localLastVisit = session.firstEventTime;

                    JSONArray events = session.getMeta();
                    for(Object event: events){
                        int row = orcBatch.size++;
                        orcUuid.setVal(row, uuid.getBytes(StandardCharsets.UTF_8));
                        orcVisit_no.vector[row] = localVisitCount;
                        orcMeta.setVal(row, ((JSONObject)event).toString().getBytes(StandardCharsets.UTF_8));
                        if (orcBatch.size == orcBatch.getMaxSize()) {
                            orcWriter.addRowBatch(orcBatch); // Writing to memory
                            orcBatch.reset(); // resetting the Vector batch to empty
                        }
                    }
                }
                kafkaOffset = session.offset;
            }
            if(firstTime) {
                cqlSession.execute(batchStatementBuilder
                        .addStatement(newUserInsertable.bind(uuid, localVisitCount, localFirstVisit, localLastVisit))
                        .build());
                userCount++;
//                log.info("[1] Inserting new user to cassandra");
                firstTime = false;
            }else{
                cqlSession.execute(batchStatementBuilder
                        .addStatement(updateInsertable.bind(localVisitCount, localLastVisit, uuid))
                        .build());
            }

//             Cache remove as of processed
             jedis.ltrim(key,1, -1);
        }

        return userCount;
    }


    private void commitToKafka(KafkaConsumer<String, String> kafkaConsumer){
        Map<TopicPartition, OffsetAndMetadata> commitOffset = new HashMap<>();

        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION);
//        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        System.out.println(kafkaOffset+1);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(kafkaOffset+1);
        commitOffset.put(topicPartition, offsetAndMetadata);

        kafkaConsumer.commitSync(commitOffset);
        System.out.println("offsetCommited");
    }

    private void processFilesPerday() throws IOException {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(conf);
//        fileSystem.

    }

    public void run() throws ParseException {
        final String REDIS_ADDR = "localhost";
        final int REDIS_PORT = 6379;
        int count = 0;
        System.out.println("Started...");
        final String KAFKA_CONSUMER_GROUP_ID = "hadoop_data_group_2";
        final String KAFKA_BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092";

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.setBoolean("orc.overwrite.output.file", true);

        TypeDescription schema = TypeDescription.fromString("struct<uuid:string,visit_no:bigint,meta:string>"); //Changed
        OrcFile.WriterOptions wo = OrcFile.writerOptions(conf)
                .setSchema(schema);

        try(KafkaConsumer<String, String> kafkaConsumer = initializeKafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP_ID );
            Jedis jedis = new Jedis(REDIS_ADDR, REDIS_PORT);
            CqlSession session = CqlSession.builder()
                    .withKeyspace("test")
                    .withLocalDatacenter("my-datacenter-1")
                    .build();
            ){
            String orcPath = "hdfs://localhost:9000/perday/temp.orc";
            Writer writer = OrcFile.createWriter(new Path(orcPath),
                    wo);
            VectorizedRowBatch batch = schema.createRowBatch();
            BytesColumnVector uuid = (BytesColumnVector) batch.cols[0];
            LongColumnVector visit_no = (LongColumnVector) batch.cols[1];
            BytesColumnVector meta = (BytesColumnVector) batch.cols[2];

            prepareStatements(session);
            log.info("[*] Connected to all clients...");
            long atTheMomemtUuidsLength = jedis.llen("uuids");

            for(long i = 0;i<atTheMomemtUuidsLength;i++){
                List<String> keys = jedis.lrange("uuids", 0, 0);
                for (String key : keys){
                    count+=processUser(jedis, writer, session, key, batch, uuid, visit_no, meta);
                    jedis.ltrim("uuids", 1, -1);
                }
            }
            log.info("Total updated user {}.", count);
            log.info("Total no. of users current moment {}", atTheMomemtUuidsLength);
            System.out.println(count);
            // Closing Orc Writer
            writer.close();
            System.out.println("Writer closed");

            if(count>0) {
                conf.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem fileSystem = FileSystem.get(conf);
                Path fileName = new Path("hdfs://localhost:9000/perday/appendable-records-" + UUID.randomUUID() + ".orc");
                fileSystem.rename(new Path("hdfs://localhost:9000/perday/temp.orc"), fileName);
                fileSystem.close();
            }

            // Commiting to Kafka after closing writer
            if(kafkaOffset!=-1)
                commitToKafka(kafkaConsumer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
