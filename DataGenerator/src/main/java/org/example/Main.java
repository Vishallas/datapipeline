package org.example;


import java.awt.*;
import java.io.*;
import java.security.NoSuchAlgorithmException;

public class Main {
    static final int THREAD_COUNT = 5;

    private static void createFile() throws NoSuchAlgorithmException, IOException {
        RandomStringGenerator rd = new RandomStringGenerator();
        rd.createFile();
    }
    private static void testIp() throws NoSuchAlgorithmException {
        RandomStringGenerator rd = new RandomStringGenerator();
        for(String ip : rd.createIpArray(50)){
            System.out.println(ip);
        }
    }

    public static void main(String[] args) throws  Exception{
//        testIp();
//        createFile();
        String fileName = "/home/vishal-pt7653/Documents/Project-assignment/datapipeline/DataGenerator/rawData.csv";
        DataProcessor dataProcessor = new DataProcessor();
        dataProcessor.processFile(fileName);
    }
}