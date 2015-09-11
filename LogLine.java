package code;

import nl.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringEscapeUtils;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
/**
 * This is the main Log line class. Any specific types of log line can inherit/override this one. It has a data structure called outputs which is a map with a string key and value pair containing the information about each log line.
 */
public class LogLine {
    protected String logline;
    protected Log log;
    protected Map<String,String> outputs;
    protected List<String> lineItems;
    protected ResultSet splitters;
    protected int cpcode;
    public LogLine(Log log, String logline, String breaker, int cpcode, ResultSet splitters){
        //Add one item to the log line
        Log.addLine();
        this.log = log;
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

    /**
     * This handles splitting each line by the breaker that is specified in the log file type.
     * @param breaker The string the split the file on.
     */
    private void splitLine(String breaker) {
        lineItems = Arrays.asList(logline.split(breaker));
    }

    /**
     * This handles processing each line and assigning the basic data to the outputs data structure. It then generates more data for the outputs map by breaking down the ip address and user agent.
     */
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
            outputs.put("log_file", log.getName());
            outputs.put("log_line", Integer.toString(Log.getLine()));
            //Now get information about the user agent
            uaSplit();
            //Now get information about the ip address
            ipSplit();

    }

    /**
     * This handles getting specific information about the user agent from the user agent string and assigning it to the outputs data structure.
     */
    private void uaSplit() {
        UserAgent agent = UserAgent.parseUserAgentString((String) outputs.get("user_agent"));
        outputs.put("browser", agent.getBrowser().getName());
        outputs.put("device",agent.getOperatingSystem().getDeviceType().getName());
        outputs.put("operating_system", agent.getOperatingSystem().getName());//**/

    }

    /**
     * This handles the generation of the ip number and assigning it to the outputs data structure.
     */
    private void ipSplit() {
        IP ip = new IP(outputs.get("ip_address"));
        outputs.put("ip_number",Long.toString(ip.getIpNumber()));
    }
    /**
     * This handles adding a segment back in to a master download
     * @param line The segment line that is being added back in to the master
     */
    public void addSegment(LogLine line) {
        // adding segment information to the logline
        outputs.put("throughput", Double.toString(Double.parseDouble(outputs.get("throughput")) + Double.parseDouble(line.getOutputs().get("throughput"))));
        outputs.put("scbytes", Integer.toString(Integer.parseInt(outputs.get("scbytes")) + Integer.parseInt(line.getOutputs().get("scbytes"))));
        outputs.put("duration", Integer.toString(Integer.parseInt(outputs.get("duration")) + Integer.parseInt(line.getOutputs().get("duration"))));
        outputs.put("segment_count", Integer.toString(Integer.parseInt(outputs.get("segment_count")) + 1));
    }
}
