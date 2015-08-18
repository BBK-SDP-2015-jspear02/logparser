package code;

import nl.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringEscapeUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class LogLine {
    protected String logline;
    protected Map<String,String> outputs;
    protected List<String> lineItems;
    protected ResultSet splitters;
    protected int cpcode;
    public LogLine(String logline, String breaker, int cpcode, ResultSet splitters){
        //Add one item to the log line
        Log.addLine();
        this.logline = logline;
        this.cpcode = cpcode;
        this.splitters = splitters;
        outputs = new HashMap<String,String>();
        //Split the line by the breaker that has been passed in
        splitLine(breaker);

    }
    public Map<String,String> getOutputs(){
        return this.outputs;
    }
    private void splitLine(String breaker) {
        lineItems = Arrays.asList(logline.split(breaker));

    }

    protected void processLine(){
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
            //First calculate the throughput from the scbytes
            Double tput = Double.parseDouble(outputs.get("scbytes")) / 1048576;
            outputs.put("throughput",Double.toString(tput));
            outputs.put("segment_count","1");

            urlSplit();
            uaSplit();
            ipSplit();

            for (Map.Entry<String, String> entry : outputs.entrySet()) {
                String key2 = entry.getKey();
                Object value = entry.getValue();
                System.out.println(key2 + " : " + value);
            }

    }

    private void uaSplit() {
       // UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
        UserAgent agent = UserAgent.parseUserAgentString((String) outputs.get("user_agent"));
        outputs.put("browser", agent.getBrowser().getName());
        outputs.put("device",agent.getOperatingSystem().getDeviceType().getName());
        outputs.put("operating_system", agent.getOperatingSystem().getName());//**/

    }

    private void ipSplit() {
        IP ip = new IP((String) outputs.get("ip_address"));
        outputs.put("ip_number",Long.toString(ip.getIpNumber()));
        outputs.put("country", ip.get("country"));
        outputs.put("region", ip.get("region"));
        outputs.put("city", ip.get("city"));
    }

    protected void urlSplit(){
        String[] basicInfo = urlSplitBasic();

        outputs.put("path",basicInfo[0]);

        //Create the directory array
        String[] dirArr = basicInfo[0].split("/");


        outputs.put("dir1", (dirArr.length > 2) ? dirArr[1] : "");
        outputs.put("dir2", (dirArr.length > 3) ? dirArr[2] : "");

        outputs.put("file_ref",dirArr[dirArr.length-1]);
        outputs.put("directories", buildString(dirArr,"/"));

        fileSplit();

    }

    protected String[] urlSplitBasic() {
        //First clean up the url of any unusual characters
        String urlNoHtml = StringEscapeUtils.unescapeHtml(outputs.get("full_url"));
        //Then break the url up into querystring and stem
        String[] urlArr = urlNoHtml.split("\\?");

        outputs.put("url", urlArr[0]);
        outputs.put("querystring",(urlArr.length > 1) ? urlArr[1] : "");


        //Now get the url splitters which are relevant to this cpcode
        String[] rtnArray = new String[2];

        try {
            while (splitters.next()) {
                System.out.println(splitters.getString("split"));
                if(outputs.get("url").toString().indexOf(splitters.getString("split")) != -1) {
                    //Remove the base url
                    System.out.println(splitters.getString("split"));
                    rtnArray[0] = outputs.get("url").toString().replace(splitters.getString("split"),"");
                    rtnArray[1] = splitters.getString("client");
                    break;
                }
            }
            //Move back to the front of the recordset
            splitters.beforeFirst();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            rtnArray[0] = "";
            rtnArray[1] = "";
        }
        return rtnArray;
    }

    protected void fileSplit() {
        String[] fileArr = outputs.get("file_ref").split("\\.");
        outputs.put("file_type", (fileArr.length > 0) ? fileArr[fileArr.length-1] : "UNKNOWN");
        outputs.put("file_name",buildString(fileArr,"\\."));
    }

    //A method for rebuilding a string after it has been broken apart into an array - similar to implode in php
    protected String buildString(String[] strArr, String join) {
        String joinedOutput = "";
        for (int i = 0; i < strArr.length; i++) {
            joinedOutput += (i == (strArr.length - 1)) ? "" : strArr[i] + ((i == strArr.length - 2) ? "" : join);
        }
        return joinedOutput;
    }

    public void addSegment(LogLine line) {
        // adding segment information to the logline
        outputs.put("throughput", Double.toString(Double.parseDouble(outputs.get("throughput")) + Double.parseDouble(line.getOutputs().get("throughput"))));
        outputs.put("scbytes", Integer.toString(Integer.parseInt(outputs.get("scbytes")) + Integer.parseInt(line.getOutputs().get("scbytes"))));
        outputs.put("duration", Integer.toString(Integer.parseInt(outputs.get("duration")) + Integer.parseInt(line.getOutputs().get("duration"))));
        outputs.put("segment_count", Integer.toString(Integer.parseInt(outputs.get("segment_count")) + 1));
    }
}
