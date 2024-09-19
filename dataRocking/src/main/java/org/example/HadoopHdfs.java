package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class HadoopHdfs {
    public static void appendToFile(){
        String hdfsFilePath = "/append/doc1.txt";

        // Data to append
        String dataToAppend = "This is the data to append.\n";

        // Configuration for HDFS
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        configuration.setBoolean("dfs.support.append", true);
        // Add additional configuration as needed, e.g., setting the HDFS URI

        FileSystem fs = null;
        BufferedWriter bufferedWriter = null;
        OutputStream os = null;

        try {
            // Get the HDFS file system
            fs = FileSystem.get(configuration);
            Path path = new Path(hdfsFilePath);

            // Check if the file exists
            if (fs.exists(path)) {
                // Append data to the file
                os = fs.append(path);
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(os));

                bufferedWriter.write(dataToAppend);
                System.out.println("Data appended successfully.");
            } else {
                System.out.println("File does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Close resources
            IOUtils.closeStream(bufferedWriter);
            IOUtils.closeStream(os);
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void writeTofile() throws IOException {
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.defaultFS", "hdfs://localhost:9000");
        try(FileSystem fileSystem = FileSystem.get(hadoopConfig);) {
            String filePath = "/append/doc1.txt";
            Path hdfsPath = new Path(filePath);
//        fShell.setrepr((short) 1, filePath);
//            FSDataOutputStream fileOutputStream = null;


            try (FSDataOutputStream fileOutputStream = fileSystem.create(hdfsPath);) {
                fileOutputStream.writeBytes("creating and writing into file\n");
            }
        }


    }
}
