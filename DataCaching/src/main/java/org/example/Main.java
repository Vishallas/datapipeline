package org.example;

public class Main {

    public static void main(String[] args) {
        DataProcessor dataProcessor = new DataProcessor();
        try {
            dataProcessor.processTopic("hadoop_data");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}






