package org.example;


import java.io.*;
import java.security.NoSuchAlgorithmException;

public class Main {
    static final int THREAD_COUNT = 5;

    private static void createFile() throws NoSuchAlgorithmException, IOException {
        FileGenerator rd = new FileGenerator();
        rd.createFile();
    }

    public static void main(String[] args) throws  Exception{

//        createFile();
        String fileName = "/home/vishal-pt7653/Documents/Project-assignment/datapipeline/DataGenerator/rawData.csv";
        DataProcessor dataProcessor = new DataProcessor();
//        dataProcessor.processFile(fileName);
        System.out.println(dataProcessor.md5UUID("180.0.0.0","Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML)"));

    }
}