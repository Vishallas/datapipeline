package org.example;

import com.opencsv.CSVWriter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.io.IOException;
import java.io.FileWriter;



class FileGenerator {
    private final MessageDigest md;
    private final DateTimeFormatter formatter;
    private final String[] userAgentList;
    private final String[] urlList;
    private final Random random;
    private Instant currentTime= null;
    private final ZoneId zoneId;
    private final int IP_NEED = 50;
    private final int USERAGENTS_NEED = 20;
    private int ipCount = 0;
    private int userAgentCount = 0;
    private final String[] ips;
    private final int USER_COUNT;

    FileGenerator() throws NoSuchAlgorithmException {
        this.md = MessageDigest.getInstance("md5");
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        this.userAgentList = assignUserAgent();
        this.urlList = assignUrl();
        this.random =new Random();
        this.zoneId = ZoneId.of("Asia/Kolkata");
        this.ips = createIpArray(IP_NEED);
        this.USER_COUNT = IP_NEED*USERAGENTS_NEED;
    }

    private String[] assignUserAgent(){
        return new String[] {
                "Mozilla/5.0 (iPad; CPU OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12F69 Safari/600.1.4",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.94 Chrome/37.0.2062.94 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko"
                ,"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9"
                ,"Mozilla/5.0 (iPad; CPU OS 8_4_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H321 Safari/600.1.4"
                ,"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.10240"
                ,"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
                ,"Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko"
                ,"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko"
                ,"Mozilla/5.0 (Windows NT 10.0; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:40.0) Gecko/20100101 Firefox/40.0"
                ,"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/7.1.8 Safari/537.85.17"
                ,"Mozilla/5.0 (iPad; CPU OS 8_4 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H143 Safari/600.1.4"
            };
    }

    private String[] assignUrl(){
        return new String[] {
                "zoho.com/phonebridge/developer/v3/call-control.html",
                "zoho.com/in/inventory/inventory-dictionary/unit-of-measure.html",
                "zoho.com/expense/#/trips/3260118000001599155",
                "help.zoho.com/portal/en/kb/people/search/leave carry",
                "help.zoho.com/portal/en/kb/people/zoho-people-4-0/employee-handbook-zoho-people-4-0/employee-leave/articles/zoho-people-employee-leave",
                "help.zoho.com/portal/en/kb/people/administrator-guide/leave/overview/articles/leave-service-overview#What_is_Leave_Service_in_Zoho_People",
                "help.zoho.com/portal/en/kb/people/administrator-guide/introduction/articles/what-is-zoho-people#Services_and_features_of_Zoho_People",
                "zoho.com/in/books/help/transaction-approval/#approving",
                "zoho.com/sheet/analyze.html",
                "zoho.com/ipr-complaints.html",
                "zoho.com/abuse-policy/",
                "zoho.com/commerce/index1.html",
                "store.zoho.in/html/store/index.html#subscription",
                "zoho.com/id/forms/",
                "help.zoho.com/portal/en/kb/vault/user-guide/sharing-passwords/articles/vault-add-passwords-notes-documents#Creating_custom_categories",
                "zoho.com/ae/books/pricing/pricing-comparison.html",
                "help.zoho.com/portal/en/kb/people/administrator-guide/attendance-management/settings/articles/breaks#Configuring_breaks",
                "zoho.com/in/expense/#/expensereports/1471609000000640099",
                "zoho.com/projects/project-milestones.html",
                "help.zoho.com/portal/en/kb/workdrive/migrations/google/articles/migrate-from-g-suite-drive-to-zoho-workdrive#i_Create_your_own_Google_Cloud_app"
        };

    }

    private String randUrl(){
        int randomNumber = random.nextInt(urlList.length);
        return urlList[randomNumber];
    }

    private String[] userCombo(){
        String[] data = new String[2];
        if(userAgentCount==USERAGENTS_NEED) {
            userAgentCount = 0;
            ipCount++;
        }
        data[0] = ips[ipCount];
        System.out.println(ipCount +" " + userAgentCount);
        data[1] = userAgentList[userAgentCount++];
        return data;
    }

    public String[] createIpArray(int n){
        String[] array = new String[n];
        int h = 0;
        int f = 181;
        while (f<256){
            int s = 0;
            while (s<256){
                int t = 0;
                while (t<256){
                    int ff = 0;
                    while (ff<256){
                        array[h] = String.format("%d.%d.%d.%d", f, s, t, ff);
                        ff++;
                        h++;
                        if (h==n)
                            return array;
                    }
                    t++;
                }
                s++;
            }
            f++;
        }

        return array;
    }



    private String getTime(){
        if(this.currentTime == null){
            this.currentTime = Instant.now();
        }else {
            long maxIntervalMillis = 7 * 60 * 1000;
            // Minimum interval in milliseconds
            long minIntervalMillis = 1000;
            long intervalMillis = minIntervalMillis + (long) (random.nextDouble() * (maxIntervalMillis - minIntervalMillis));
            this.currentTime = this.currentTime.plusMillis(intervalMillis);
        }

        return formatter.format(currentTime.atZone(zoneId));
    }


    public void createFile() throws IOException {

        String fileName = "rawData1.csv";
        List<String[]> data = new ArrayList<>();
        Instant Time = Instant.now();
        for(int i = 0; i<USER_COUNT;i++) {
            String[] ip_useragent = userCombo();
            currentTime = Time;
            for (int j = 0; j < 20; j++) {
                data.add(new String[]{ip_useragent[0], ip_useragent[1], randUrl(), getTime()});
            }

        }

        char customDelimiter = '|';
            try (CSVWriter writer = new CSVWriter(new FileWriter(fileName),
                    customDelimiter,
                    '\0',
                    '\0',
                    CSVWriter.DEFAULT_LINE_END)) {
                writer.writeAll(data);
            }
    }
}
