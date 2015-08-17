package code;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.lang.StringEscapeUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class  Log {
    //Splitters are the strings that mark the end of the generic info
    protected ResultSet logSplitters;
    //Each log has a unique name, cpcode, domain and logtype
    protected int cpcode;
    protected ResultSet liveFix;
    protected String logName,logType, domain, cdn, deliveryType, client, breaker;
    protected List<? extends LogLine> logLines;
    protected static int lineCount = 0;
    public Log(String logname,String logType,ResultSet logDetails, ResultSet logSplitters, ResultSet liveFix) throws SQLException,IOException{
        this.logName = logname;
        this.logType = logType;
        this.liveFix = liveFix;
        this.logSplitters = logSplitters;
        this.cpcode = logDetails.getInt("cpcode");
        this.domain = logDetails.getString("domain");
        this.cdn = logDetails.getString("cdn");
        this.deliveryType = logDetails.getString("delivery_type");
        this.client = logDetails.getString("client");
        //Defines what the line items are split by - usually tab but sometimes space.
        this.breaker = logDetails.getString("split_characters");
        System.out.println(client);
        readLog();
    }

    protected void readLog() throws IOException{
        lineCount = 0;
        List<String> stringLines = LogReader.OpenReader(logName);
         //Convert the strings into objects of type logline after checking that they aren't header lines
        try {
            this.logLines = stringLines.stream().filter(x -> (!(isHeader(x)))).map(y -> LogLineFactory.makeLogLine(y, breaker, cpcode, logSplitters,liveFix,logType)).collect(Collectors.toList());
        } catch (Exception ex) {
            System.out.println("Problem with log file " + logName + " on line " + lineCount + ".");
        }
    }



    protected void addLogFields(Map<String,String> outputs){
        outputs.put("cdn",cdn);
        outputs.put("cpcode",Integer.toString(cpcode));
        outputs.put("delivery_method", deliveryType);
        outputs.put("domain",domain);

    }
    protected void insertToDB(LogLine line) {
        //Add the fields that are determined by the log file rather than the log line
        addLogFields(line.getOutputs());
        String sql = "INSERT INTO logdata (";
        String fields =  "";//line.outputs.keySet().stream().map(x -> x + ",").forEach( inputs );
        String inputs = "";
        for (Map.Entry<String, String> entry : line.getOutputs().entrySet())
        {
            fields += entry.getKey() + ",";
            inputs += fieldType(entry.getKey(),entry.getValue()) + ",";
           // System.out.println(entry.getKey() + "/" + entry.getValue());
        }

        if (!(fields.equals("cpcode,delivery_method,cdn,"))) {
            sql += fields.substring(0, fields.lastIndexOf(",")) + ") VALUES (" + inputs.substring(0, inputs.lastIndexOf(",")) + ")";
            RunIt.db.insert(sql);
        }
        System.out.println(inputs);
       System.out.println(fields);
    }

    public static String fieldType(String key,Object item) {

        switch(key) {
            case "ip_number":
            case "scbytes":
            case "status":
                return (item == null) ? "" : item.toString();
            default:
                return (item == null) ? "''" : "'"+item.toString() + "'";
        }

    }
    public static void addLine() {
        lineCount++;
    }

    public static int getLine() {
        return lineCount;
    }

    protected boolean isHeader(String line) {
        String[] lineSplit = line.split("\\s+");
        switch (lineSplit[0]){
            case "#Version:":
            case "#Fields:":
                return true;
            default:
                return false;
        }

    }


}
