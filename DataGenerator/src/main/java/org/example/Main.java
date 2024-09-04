package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    static final int THREAD_COUNT = 5;

    //static final String TOPIC = "hadoop_data";


    public static void main(String[] args) throws  Exception{
        final Logger log = LoggerFactory.getLogger(Main.class);
        log.info("Logger initialized");

//        System.out.println(ipGenerator());
//        Thread[] threads = new Thread[THREAD_COUNT];
//        for (int i = 0; i < THREAD_COUNT; i++) {
//            threads[i] = new Thread(new ProducerUtil(i));
//            log.info("[ ] Thread " + i + " Created.");
//            threads[i].start();
//            log.info("[ ] Thread " + i + " Started.");
//        }
//
//        try {
//            for (Thread t : threads)
//                t.join();
//        } catch (InterruptedException e) {
//            log.error("Exception : ", e);
//        } finally {
//            log.info("Closing Producer");
//        }
    }
}