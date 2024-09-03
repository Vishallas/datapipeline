package org.example;

import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.HashMap;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static final String REDIS_KEY = "test";
    private static final String REDIS_ADDRESS = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final long WAIT_TIME_MS = 1 * 10 * 1000; // min, sec, ms

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private void trans(){
        try(Jedis jedis = new Jedis(REDIS_ADDRESS, REDIS_PORT)) {
            Transaction transaction = jedis.multi();
            Response<Long> countResponse = transaction.llen(REDIS_KEY);
            // To do the max size of 2^32;
//            Response<List<String>> dataResponse = transaction.rpop(REDIS_KEY,  countResponse.get().intValue());
            Response<List<String>> dataResponse = transaction.rpop(REDIS_KEY, countResponse.get().intValue());

            transaction.exec();

            List<String> data = dataResponse.get();
        }
    }
    private static void dataGenerator(){
        JSONObject data = new JSONObject();

        data.put("uuid","000028e1-3e58-498a-8f4e-67a3fa7dbe89_d587"); // UUID
        data.put("visits",1); // Visit Count
        data.put("sid","-8.96494E+018");
        data.put("eventtime","2024-08-31 00:42:48"); // Event Time
        data.put("uvid","cb07f9bd-8dc1-4096-a657-6f19008f2174-2"); //UVID
        data.put("ip","182.65.217.47"); //IP
        data.put("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36");
        data.put("pageurl_trim","zoho.com/mail/"); //pageurl_trim

        String temp = data.toString();
        try(Jedis jedis = new Jedis(REDIS_ADDRESS, 6379)){
            for(int i = 0;i<REDIS_PORT;i++){
                jedis.lpush(REDIS_KEY, temp);
            }
        }
    }

    private static List<String> valueFetcher(){
        try(Jedis jedis = new Jedis(REDIS_ADDRESS, 6379)){
            long l = jedis.llen(REDIS_KEY);
            List<String> data = jedis.rpop(REDIS_KEY, (int)l);
//            data.forEach(log::info);
            log.info("Data received LEN : {}", data.size());
            return data;
        }catch (Exception e){
            log.error("Error fetching KEY : "+ REDIS_KEY, e);
            return null;
        }
    }

    private static void dataConvert(List<String> listData){
        listData.forEach((data)->{
            JSONObject json = new JSONObject(new JSONTokener(data));

        });
    }
    public static void main(String[] args) {


        try {
            while (true) {
                List<String> data = valueFetcher();
                if (data != null) {
                    // Campare data;
                    log.info("Processing Data");
                }
                dataGenerator();
                try {
                    log.info("Waiting for {} ms", WAIT_TIME_MS);
                    Thread.sleep(WAIT_TIME_MS);
                } catch (InterruptedException ie) {
                    log.error("Thread Interrupted Error ", ie);
                    break;
                }
            }
        }catch (Exception e){
            log.error("Error occured Program exits ",e);
        }
    }
}