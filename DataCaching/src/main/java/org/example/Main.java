package org.example;

import org.apache.kafka.clients.consumer.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class Main {
    static void dataSorter(List<String> data){}
    private static final int THREAD_COUNT = 5;
    private static final String TOPIC = "hadoop_data";

    public static void main(String[] args){

        TreeMap<String, String> map = new TreeMap<>();
        map.put("0000d01e-f7b3-46a9-9112-994cd713e56f_d587 2024-08-31 08:16:45", "Value1");
        map.put("00018e63-0fa1-43d5-ae34-0d188101363f_d263 2024-08-31 08:16:46", "Value2");
        map.put("0000d01e-f7b3-46a9-9112-994cd713e56f_d587 2024-08-31 08:16:46", "Value3");
        map.put("00018e63-0fa1-43d5-ae34-0d188101363f_d263 2024-08-31 08:16:42", "Value4");

        map.values().forEach(System.out::println);

        String prefix = "0000d01e-f7b3-46a9-9112-994cd713e56f_d587";

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






