package org.example;

import org.apache.kafka.clients.consumer.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {
    private static final int THREAD_COUNT = 5;
    private static final String TOPIC = "hadoop_data";

    public static void main(String[] args){

        final Logger log = LoggerFactory.getLogger(Main.class);

        Thread[] threads = new Thread[THREAD_COUNT];

        for(int i = 0;i<THREAD_COUNT;i++){
            threads[i] = new Thread(new ConsumerUtil(i, TOPIC));
            threads[i].start();
            log.info(threads[i].getName() + "Started.....");
        }

        try {
            for (Thread t : threads)
                t.join();
        } catch (InterruptedException e) {
            log.error("Exception Joining Threads ", e);
        }
    }
}