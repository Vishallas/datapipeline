package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;


public class Main {
    static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static void sendBlob(int id){

//        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("name", "vishal");
//
//        String data = jsonObject.toString();

        try(CqlSession session = CqlSession.builder()
                .withKeyspace("test")
                .withLocalDatacenter("my-datacenter-1")
                .build()) {

            PreparedStatement preparedStatement = session.prepare(
                    QueryBuilder.insertInto("Appendable")

                            .value("uuid", QueryBuilder.bindMarker())
                            .value("visit_count", QueryBuilder.bindMarker())
                            .value("first_visit", QueryBuilder.bindMarker())
                            .value("last_visit", QueryBuilder.bindMarker())
                            .build()
            );
            String firstVisit = "2024-09-09 12:25:44";
            String lastVisit = "2024-09-09 12:29:22";

            Instant first = sdf1.parse(firstVisit).toInstant();
            Instant last = sdf1.parse(lastVisit).toInstant();

            session.execute(preparedStatement.bind("13lk23kljlkj", 1L , first, last));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
    private static void getJson(){
        try(CqlSession session = CqlSession.builder()
                .withKeyspace("test")
                .withLocalDatacenter("my-datacenter-1")
                .build()) {

            PreparedStatement preparedStatement = session.prepare(
                    QueryBuilder.selectFrom("appendable")
                            .column("last_visit")
                            .whereColumn("uuid")
                            .isEqualTo(QueryBuilder.bindMarker())
                            .build()
            );
            Row row = session.execute(preparedStatement.bind("13lk23kljlkj")).one();
            System.out.println(row.getInstant("last_visit").atZone(ZoneId.of("Asia/Kolkata")).toString());
        }
    }
    private static void checkResultRest(){
        CqlSession cqlSession = CqlSession.builder()
                .withKeyspace("test")
                .withLocalDatacenter("my-datacenter-1")
                .build();
        PreparedStatement getLastDataOfUser = cqlSession.prepare(
                QueryBuilder.selectFrom("insertable")
                        .columns("visit_count", "last_visit")
                        .whereColumn("uuid")
                        .isEqualTo(QueryBuilder.bindMarker("uuid"))
                        .build());
        Row rs = cqlSession.execute(getLastDataOfUser.bind("3933dec6-ee27-90a8-e775-9e04459c4251")).one();
        System.out.println(rs == null);
    }
    public static void main(String[] args) throws ParseException {
//        sendBlob(1);
//        getJson();
//        checkResultRest();
        DataProcessorUtil dp = new DataProcessorUtil();
        dp.run();
    }
}