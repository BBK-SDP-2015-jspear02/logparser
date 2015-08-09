package code;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class LogLine {
    String logline;
    Map<String,Object> outputs;
    List<String> lineItems;
    private ResultSet splitters;
    private String ipaddress,date,time,url,qs,client,path,directories,dir1,dir2,viewId,referrer,fileType, userAgent,browser,os, type,view,request;
    private int ipnumber, status, cpcode;
    private double throughput;
    public LogLine(String logline, String breaker, int cpcode, ResultSet splitters){
        //Add one item to the log line
        Log.addLine();
        this.logline = logline;
        this.cpcode = cpcode;
        this.splitters = splitters;
        outputs = new HashMap<String,Object>();
        //Split the line by the breaker that has been passed in
        splitLine(breaker);
        //Now process this line
        processLine();
    }
    private void splitLine(String breaker) {
        lineItems = Arrays.asList(logline.split(breaker));

    }
    private void processLine(){
        if (Log.getLine() > 2) {
            for (int i = 0; i < lineItems.size(); i++) {
                String key;
                switch (i) {
                    case 0:
                        key = "date";
                        break;
                    case 1:
                        key = "time";
                        break;
                    case 2:
                        key = "ip_address";
                        break;
                    case 3:
                        key = "type";
                        break;
                    case 4:
                        key = "full_url";
                        break;
                    case 5:
                        key = "status";
                        break;
                    case 6:
                        key = "scbytes";
                        break;
                    case 7:
                        key = "duration";
                        break;
                    case 9:
                        key = "user_agent";
                        break;
                    default:
                        key = "";
                        break;
                }
                if (!(key == "")) {
                    outputs.put(key, lineItems.get(i));
                }

            }
            //All information gathered. Now calculate extra fields.
            urlSplit();
            uaSplit();
            ipSplit();
            for (Map.Entry<String, Object> entry : outputs.entrySet()) {
                String key2 = entry.getKey();
                Object value = entry.getValue();
                System.out.println(key2 + " : " + value);
            }
        }
    }

    private void uaSplit() {
        UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
        ReadableUserAgent agent = parser.parse((String) outputs.get("user_agent"));
        outputs.put("browser", agent.getName());
        outputs.put("device",agent.getDeviceCategory().getName());
        outputs.put("os", agent.getOperatingSystem().getName());
    }

    private void ipSplit() {
        IP ip = new IP((String) outputs.get("ip_address"));
        outputs.put("ip_number",ip.getIpNumber());
        outputs.put("country", ip.get("country"));
        outputs.put("region", ip.get("region"));
        outputs.put("city", ip.get("city"));
    }

    private void urlSplit(){
        //First clean up the url of any unusual characters

        //Then break the url up into querystring and stem
        String[] urlArr = outputs.get("full_url").toString().split("\\?");

        outputs.put("url", urlArr[0]);
        outputs.put("querystring",(urlArr.length > 1) ? urlArr[1] : "");


        //Now get the url splitters which are relevant to this cpcode
        String urlNoBase = "";
        String client = "";
        try {
            while (splitters.next()) {
                System.out.println(splitters.getString("split"));
                if(outputs.get("url").toString().indexOf(splitters.getString("split")) != -1) {
                    //Remove the base url
                    System.out.println(splitters.getString("split"));
                    urlNoBase = outputs.get("url").toString().replace(splitters.getString("split"),"");
                    client = splitters.getString("client");
                    splitters.first();
                    break;
                }
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        //Create the directory array
        String[] dirArr = urlNoBase.split("/");
        if (client.equals("STANDARD")) {
            outputs.put("client",dirArr[0]);
            outputs.put("dir1", (dirArr.length > 2) ? dirArr[1] : "");
            outputs.put("dir2", (dirArr.length > 3) ? dirArr[2] : "");
        }



    }
}
