package org.example;

import org.apache.kafka.clients.consumer.*;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.*;


public class Main {
    private static final String REDIS_ADDR = "localhost";
    private static final int REDIS_PORT = 6379;

    static void dataSorter(List<String> data){}
    private static final int THREAD_COUNT = 5;
    private static final String TOPIC = "hadoop_data";

    public static void main(String[] args){

        try (Jedis jedis = new Jedis(REDIS_ADDR, REDIS_PORT)) {
            
        }

//        SortedMap<String, String> result = map.subMap(prefix, prefix + Character.MAX_VALUE);
//        result.for_each((key, val) -> System.out.println(key + " : " + val));

//        final Logger log = LoggerFactory.getLogger(Main.class);
//
//        Thread[] threads = new Thread[THREAD_COUNT];
//
//        for(int i = 0;i<THREAD_COUNT;i++){
//            threads[i] = new Thread(new ConsumerUtil(i, TOPIC));
//            threads[i].start();
//            log.info(threads[i].getName() + "Started.....");
//        }
//
//        try {
//            for (Thread t : threads)
//                t.join();
//        } catch (InterruptedException e) {
//            log.error("Exception Joining Threads ", e);
//        }
    }
}






