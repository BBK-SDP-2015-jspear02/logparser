package code;

import java.io.IOException;
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
    protected String logName, domain, cdn, deliveryType, client, breaker;
    protected List<LogLine> logLines;
    protected static int lineCount = 0;
    public Log(String logname,ResultSet logDetails, ResultSet logSplitters) throws SQLException,IOException{
        this.logName = logname;
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
        analyseLog();
    }

    protected void readLog() throws IOException{
        lineCount = 0;
        List<String> stringLines = LogReader.OpenReader(logName);
         //Convert the strings into objects of type logline after checking that they aren't header lines
        try {
            this.logLines = stringLines.stream().filter(x -> (!(isHeader(x)))).map(y -> new LogLine(y, breaker, cpcode, logSplitters)).collect(Collectors.toList());
        } catch (Exception ex) {
            System.out.println("Problem with log file " + logName + " on line " + lineCount + ".");
        }
    }

    protected void analyseLog() {
        System.out.println("got here");
        //Now all of the loglines have been created we need to strip out the partial requests to process them as individual lines
        //1. Move all the log lines with 206 status to a seperate array
        try {
            List<LogLine> partialHits = this.logLines.stream().filter(x -> x.getOutputs().get("status").toString().equals("206")).collect(Collectors.toList());
            List<LogLine> fullHits = this.logLines.stream().filter(x -> !(x.getOutputs().get("status").toString().equals("206"))).collect(Collectors.toList());
            partialHits.stream().forEach(x-> System.out.println(x.getOutputs().get("full_url")));
            fullHits.stream().forEach(x-> insertToDB(x));
        }catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

       //
       // System.out.println("size:" + partialHits.size());
       // this.logLines.stream().forEach(x-> insertToDB(x));
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
