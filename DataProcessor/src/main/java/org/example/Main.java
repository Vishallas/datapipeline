package org.example;

import com.sun.org.apache.xerces.internal.util.ShadowedSymbolTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.*;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
            System.out.println(batch.size);
            for (int i = 0;i<batch.size;i++) {
                System.out.println(new String(uuid.vector[i], StandardCharsets.UTF_8) +" "+ visit_no.vector[i]+" "+ new String(meta.vector[i]));
            }
        }
        rows.close();

    }
    static Writer writer = null;
    static int previousMonth = 0;
    static int previousYear = 0;
    static String yearColumnName;
    static String monthColumnName;
    static String initFileName = "tmpData";
    static Path prevPath;
    static VectorizedRowBatch orcBatch;
    static BytesColumnVector orcUuid;
    static LongColumnVector orcVisit_no;
    static BytesColumnVector orcUrl;
    static TimestampColumnVector orcEventTime;
    static FileSystem fileSystem;
    static long MAX_FILE_SIZE = 10 * 1024 * 1024L;
    static OrcFile.WriterOptions writerOptions;
    static String baseFormaterString = "/testingFolder/%s=%d/%s=%d/%s";
    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    static void finishFileWriting() throws IOException {
        Path initFilePath = new Path(String.format(baseFormaterString,
                yearColumnName,
                previousYear,
                monthColumnName,
                previousMonth,
                initFileName));

        Path tempFilePath = new Path(String.format(baseFormaterString,
                yearColumnName,
                previousYear,
                monthColumnName,
                previousMonth,
                "temp.orc"));

        FileStatus fileStatus = fileSystem.getFileStatus(tempFilePath);

        if(fileStatus.getLen() > MAX_FILE_SIZE){
            System.out.println("Renaming file....");
            Path randFilePath = new Path(String.format(baseFormaterString,
                    yearColumnName,
                    previousYear,
                    monthColumnName,
                    previousMonth,
                    "appendable-records-" + UUID.randomUUID() + ".orc"));
            fileSystem.rename(tempFilePath, randFilePath);

            if(fileSystem.exists(initFilePath)) // Checking for .init file
                fileSystem.rename(initFilePath,tempFilePath);
        }else {

            if(fileSystem.exists(initFilePath)) {
                Path mergeFilePath = new Path(String.format(baseFormaterString,
                        yearColumnName,
                        previousYear,
                        monthColumnName,
                        previousMonth,
                        ".temp.orc"));
                OrcFile.mergeFiles(mergeFilePath,
                        writerOptions,
                        new ArrayList<Path>() {
                            {
                                add(tempFilePath);
                                add(initFilePath);
                            }
                        }
                );
                FileStatus[] fileStatus1 = fileSystem.listStatus(new Path(String.format("/testingFolder/%s=%d/%s=%d/",yearColumnName,
                        previousYear,
                        monthColumnName,
                        previousMonth)));
                System.out.println(String.format("Mergining files"));
                fileSystem.delete(tempFilePath);
                fileSystem.delete(initFilePath);
                fileSystem.rename(mergeFilePath, tempFilePath);
            }
        }
    }

    public static void writeToVectorBatch(String uuid, long localVisitCount, String url, String eventTime) throws IOException {
        int row = orcBatch.size++;
        orcUuid.setVal(row, uuid.getBytes(StandardCharsets.UTF_8));

        orcVisit_no.vector[row] = localVisitCount;

        orcUrl.setVal(row, url.getBytes(StandardCharsets.UTF_8));
        orcEventTime.set(row, Timestamp.valueOf(eventTime));

        if (orcBatch.size == orcBatch.getMaxSize()) {
            writer.addRowBatch(orcBatch); // Writing to memory
            orcBatch.reset(); // resetting the Vector batch to empty
        }
    }

    public static void uploadFile(String uuid, long localVisitCount, String url, String eventTime) throws IOException {
//        Configuration conf = new Configuration();
//        conf.setBoolean("orc.overwrite.output.file", true);
//        TypeDescription schema = TypeDescription.fromString("struct<uuid:string,visit_no:bigint,url:string,eventtime:timestamp>");
//        writerOptions = OrcFile.writerOptions(conf)
//                .setSchema(schema);
//        conf.set("fs.defaultFS", "hdfs://localhost:9000");
//        fileSystem = FileSystem.get(conf);
        LocalDateTime dt = LocalDateTime.parse(eventTime, formatter);
        int currentMonth = dt.getMonthValue();
        int currentYear = dt.getYear();

//        LocalDate currentDate
//                = LocalDate.parse(eventTime);
//
//        int currentMonth = currentDate.getMonthValue();
//        int currentYear = currentDate.getYear();

        if(previousMonth != 0 && previousYear != 0){
            if(previousYear == currentYear && previousMonth == currentMonth){
                System.out.println("Writing to already existing file.."+"year="+previousYear+" month="+previousMonth);
                writeToVectorBatch(uuid, localVisitCount, url, eventTime);
            }else {
                System.out.println("Writing to new directory.."+"year="+currentYear+" month="+currentMonth);
                writer.addRowBatch(orcBatch); // Writing to memory
                orcBatch.reset(); // Reset Batch
                writer.close(); // Flush previous data to writer
                finishFileWriting(); // Handle merge to temp

                Path tempFolderPath = new Path(String.format("/testingFolder/%s=%d/%s=%d",
                        yearColumnName,
                        currentYear,
                        monthColumnName,
                        currentMonth));

                boolean folderCreated = fileSystem.mkdirs(tempFolderPath); // Create Directories if not found

                //  If already has file write to initFile
                String fileName = null;
                if(fileSystem.exists(new Path(tempFolderPath, "temp.orc"))){
                    fileName = initFileName;
                    System.out.println("temp already found... at year="+previousYear+"month="+previousMonth);
                }else{
                    fileName = "temp.orc";
                }
//                String fileName = !folderCreated && fileSystem.exists(new Path(tempFolderPath, "temp.orc")) ? initFileName : "temp.orc";
                if(fileName == initFileName)
                    System.out.println("Found one");
                Path newPath = new Path(String.format(baseFormaterString, yearColumnName, currentYear, monthColumnName, currentMonth, fileName));

                writer = OrcFile.createWriter(newPath, writerOptions); // Create New Writer

                writeToVectorBatch(uuid, localVisitCount, url, eventTime);


            }
        }else {

            System.out.println("Creating new file..."+"year="+currentYear+" month="+currentMonth);
            Path tempFolderPath = new Path(String.format("/testingFolder/%s=%d/%s=%d",
                    yearColumnName,
                    currentYear,
                    monthColumnName,
                    currentMonth));

            boolean folderCreated = fileSystem.mkdirs(tempFolderPath); // Create Directories if not found

            //  If already has file write to initFile
            String fileName = null;
            if(fileSystem.exists(new Path(tempFolderPath, "temp.orc"))){
                fileName = initFileName;
                System.out.println("temp already found... at year="+previousYear+"month="+previousMonth);
            }else{
                fileName = "temp.orc";
            }
            Path newPath = new Path(String.format(baseFormaterString, yearColumnName, currentYear, monthColumnName, currentMonth, fileName));

            writer = OrcFile.createWriter(newPath, writerOptions); // Create New Writer

            writeToVectorBatch(uuid, localVisitCount, url, eventTime);
        }
        previousMonth = currentMonth;
        previousYear = currentYear;
    }

    public static void start() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.setBoolean("orc.overwrite.output.file", true);

        TypeDescription schema = TypeDescription.fromString("struct<uuid:string,visit_no:bigint,url:string,eventtime:timestamp>");
        writerOptions = OrcFile.writerOptions(conf)
                .setSchema(schema);

        fileSystem = FileSystem.get(conf);

        orcBatch = schema.createRowBatch();
        orcUuid = (BytesColumnVector) orcBatch.cols[0];
        orcVisit_no = (LongColumnVector) orcBatch.cols[1];
        orcUrl = (BytesColumnVector) orcBatch.cols[2];
        orcEventTime = (TimestampColumnVector)orcBatch.cols[3];

        yearColumnName = "year";
        monthColumnName = "month";
        uploadFile("dsfsdfsdasdf",1,"fjlsdjf/dlfjsdlk/dlkfjl", "2021-02-12 13:13:13");
        uploadFile("dsfsdf",1,"fjlsdjf/dlfjsdlk/dlkfjl", "2021-01-12 13:13:13");
        uploadFile("dsfsdf",1,"fjlsdjf/dlfjsdlk/dlkfjl", "2021-02-12 13:13:13");
        uploadFile("adfd",1,"fjlsdjf/dlfjsdlk/dlkfjl", "2021-02-12 13:13:13");
        uploadFile("dsfsdf",1,"fjlsdjf/dlfjsdlk/dlkfjl", "2021-01-12 13:13:13");
        uploadFile("dsfsadsfdsdf",1,"fjlsdjf/dlfjsdlk/dlkfjl", "2021-02-12 13:13:13");

        if(orcBatch.size!=0) {
            writer.addRowBatch(orcBatch);
            writer.close();
        }
        finishFileWriting();
    }
    public static void main(String[] args) throws ParseException, IOException {
//        DataProcessorUtil dp = new DataProcessorUtil();

//        dp.run();
//        compaction();
//        uploadFile();
        start();
//        readOrcFile();
    }
}