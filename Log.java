package code;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.lang.StringEscapeUtils;

import java.text.ParseException;
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
    protected List<String> stringLines;
    protected static int lineCount = 0;
    protected static int errorCount = 0;
    public Log(String logname,String logType,ResultSet logDetails, ResultSet logSplitters, ResultSet liveFix) throws SQLException,IOException,ParseException{
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
        readLog();
    }

    protected void readLog() throws IOException,SQLException,ParseException{
        lineCount = 0;
        errorCount = 0;
        stringLines = LogReader.OpenReader(logName);
         //Convert the strings into objects of type logline after checking that they aren't header lines
        try {
            this.logLines = stringLines.stream().
                    filter(x -> (!(isHeader(x))))
                        .map(y -> LogLineFactory.makeLogLine(this,y, breaker, cpcode, logSplitters,logType))
                            .collect(Collectors.toList());
        } catch (Exception ex) {
            RunIt.logger.writeError(logName,getLine(),ex.getMessage());
        }
    }

    protected void addLogFields(Map<String,String> outputs){
        outputs.put("cdn",cdn);
        outputs.put("cpcode",Integer.toString(cpcode));
        outputs.put("delivery_method", deliveryType);
        outputs.put("domain",domain);
        String[] setOutputs = "path,file_ref,type,file_name,referrer,directories,dir1,dir2".split(",");
        for(String output : setOutputs) {
            if (!outputs.containsKey(output))  outputs.put(output,"");
        }
    }

    public static void addError() {
        errorCount++;
    }

    protected void insertToDB(LogLine line) {
        //Add the fields that are determined by the log file rather than the log line
        addLogFields(line.getOutputs());
        String sql = "INSERT INTO logdata_temp (";
        String fields =  "";
        String inputs = "";
        for (Map.Entry<String, String> entry : line.getOutputs().entrySet())
        {
            fields += entry.getKey() + ",";
            inputs += fieldType(entry.getKey(),entry.getValue()) + ",";
        }
        fields = sql + fields.substring(0, fields.lastIndexOf(",")) + ") VALUES ";
        inputs = "(" + inputs.substring(0, inputs.lastIndexOf(",")) + ")";
        sql = fields + inputs;

       // RunIt.db.insert(this, sql);
        RunIt.db.bulkInsert(this,fields, inputs,Integer.parseInt(line.getOutputs().get("log_line")));
    }
    protected void finalizeLog() {
        if (Log.errorCount == 0) {
            //Do geo lookup

            //Insert into main logdata
            RunIt.db.operate(this, "INSERT INTO logdata SELECT * from logdata_temp;");
            //Now record that the log has been succesfully processed
            RunIt.db.insert(this,"INSERT into processed_logs (logname, line_count) VALUES ('" + this.getName() + "'," + Log.getLine() + ")");
            //Now move the log file from unprocessed to processed

            try {
                File log  = new File(RunIt.processedLogs + this.getName());
                log.renameTo(new File(RunIt.unprocessedLogs + log.getName()));

            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }

        }

        // truncate table empty
        RunIt.db.operate(this, "TRUNCATE TABLE logdata_temp");




    }

    public static String fieldType(String key,Object item) {

        switch(key) {
            case "ip_number":
            case "scbytes":
            case "status":
            case "log_line":
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

    protected String getName() { return logName;}

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
