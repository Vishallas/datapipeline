package org.example;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Arrays;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    public static void main(String[] args) {
        final String REDIS_KEY = "HadoopCache";

        try(Jedis jedis = new Jedis("localhost", 6379)){
            Transaction transaction = jedis.multi();
            Response<Long> countResponse = transaction.llen(REDIS_KEY);
            // To do the max size of 2^32;
            //Response<List<String>> dataResponse = transaction.rpop(REDIS_KEY,  countResponse.get().intValue());
//            Response<List<String>> dataResponse = transaction.lrange(REDIS_KEY, -1);

            transaction.exec();

            List<String> data = dataResponse.get();

            System.out.println(data.size());
        }
    }
}