package org.example;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;


import java.io.*;


public class Main {

    static Logger log = LoggerFactory.getLogger(Main.class);
    public static void writeDatatoHdfs() throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean("orc.overwrite.output.file", true);

        TypeDescription schema = TypeDescription.fromString("struct<uuid:string,visit_no:bigint,meta:binary>");
        OrcFile.WriterOptions wo = OrcFile.writerOptions(conf)
                .setSchema(schema);

        try(Writer writer = OrcFile.createWriter(new Path("hdfs://localhost:9000/test/data_main.orc"),
                wo);
        Jedis jedis = new Jedis("localhost:9872");) {
            VectorizedRowBatch batch = schema.createRowBatch();
            BytesColumnVector uuid = (BytesColumnVector) batch.cols[0];
            LongColumnVector visit_no = (LongColumnVector) batch.cols[1];
            BytesColumnVector meta = (BytesColumnVector) batch.cols[2];


            int r = 0;
            for (; r < 10000; ++r) {
                int row = batch.size++;
                uuid.vector[row] = "fsldkjflksdjfkl".getBytes();
                visit_no.vector[row] = r + 1;
                meta.vector[row] = "1234meta".getBytes();
                // If the batch is full, write it out and start over.
                if (batch.size == batch.getMaxSize()) {
                    System.out.println("Appended to writer at " + r);
                    log.info("[+] appended row to the writer, count = {}", r);
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                System.out.println("Appended to writer at " + r);
                writer.addRowBatch(batch);
                batch.reset();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    public static void main(String[] args) throws IOException {
//            bareMetalWrite();
//        writeDatatoHdfs();
//        tryPresto();
    }


}
