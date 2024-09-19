package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.orc.OrcFile;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


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

    public static void main(String[] args) throws ParseException, IOException {
        DataProcessorUtil dp = new DataProcessorUtil();
//        dp.run();
        compaction();
    }
}