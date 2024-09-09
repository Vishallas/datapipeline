package org.example;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.gson.JsonObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    private static void updateString(JsonObject jsonObject){
        // if data found increment the count
        // if data not found set the first and last visit time to same

    }

    private static void processData(Jedis jedis, Set<String> keys) {
        keys.forEach((key)->{

            int lent = (int)(jedis.llen(key));
            List<String> jsonStringArray = jedis.rpop(key, lent);
            JSONArray jsonArray =  new JSONArray(jsonStringArray);
            if (jsonArray.length() == 1){

            }
        });
    }

    private static void getKeys() {
        try (Jedis jedis = new Jedis(REDIS_ADDRESS, 6379)) {
//            ScanResult<String> rs = jedis.scan("user:*");
            Set<String> keys = jedis.keys("UUID:");
//            processData(jedis, keys);
//            System.out.println(rs.getResult());
        } catch (Exception e) {
            log.error("Error fetching KEY : " + REDIS_KEY, e);

        }
    }

    private static void dataConvert(List<String> listData){
        listData.forEach((data)->{
            JSONObject json = new JSONObject(new JSONTokener(data));

        });
    }
    public static void main(String[] args) {
//        Logger log = LoggerFactory.getLogger(Main.class);
        getKeys();
        try {
            while (true) {
                List<String> data = null;
//                getKeys();
                if (data != null) {
                    // Campare data;
                    log.info("Processing Data");
                }
//                dataGenerator();
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