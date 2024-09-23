package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;

public class Main {
    static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static void moveFile(FileSystem fs, Path path) throws IOException {
        long MAX_AVG_FILE_SIZE = 1024L;
        String finalPath = "/final/";
        long fileLength = fs.getFileStatus(path).getLen();
        if(fileLength>MAX_AVG_FILE_SIZE)
            fs.rename(path, new Path(finalPath+path.getName()));
    }

    private static void compaction() throws IOException {
        Configuration conf = new Configuration();
        String avgPath = "/avg/";

        String crachedFilePath = "hdfs://localhost:9000/perday/temp.orc";
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        List<Path> filePaths = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(new Path("/perday"));
        for(FileStatus fileStatus : fileStatuses){
            if(fileStatus.getPath().toString().equals(crachedFilePath)){
                System.out.println("Has a corrupted file");
                continue;
            }
            filePaths.add(fileStatus.getPath());
            System.out.println(fileStatus.getPath());
        }
        Path mergedFile = new Path(avgPath+ UUID.randomUUID()+".orc");
        OrcFile.mergeFiles(mergedFile, OrcFile.writerOptions(conf), filePaths);
        moveFile(fs, mergedFile);

        for(Path path: filePaths){
            fs.delete(path, true);
        }
    }

    public static void readOrcFile() throws IOException {


        String orcFilePath = "temp.orc"; // Change to your ORC file path

        // Configuration
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(orcFilePath),
                OrcFile.readerOptions(conf));

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        System.out.println("Hello");
        while (rows.nextBatch(batch)) {
            BytesColumnVector uuid = (BytesColumnVector) batch.cols[0];
            LongColumnVector visit_no = (LongColumnVector) batch.cols[1];
            BytesColumnVector meta = (BytesColumnVector) batch.cols[2];
            for (int i = 0;i<batch.size;i++) {
                System.out.println(new String(uuid.vector[i], StandardCharsets.UTF_8) +" "+ visit_no.vector[i]+" "+ new String(meta.vector[i]));
            }
        }
        rows.close();

    }

    public static void main(String[] args) throws ParseException, IOException {
        DataProcessorUtil dp = new DataProcessorUtil();
//        readOrcFile();
//        dp.run();
        compaction();
    }
}