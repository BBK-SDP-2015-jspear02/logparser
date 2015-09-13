package code;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * This is the main Log file format. Other, more specific log file formats inherit from this but much of the processing of the log takes place here.
 */
public abstract class  Log {
    //Splitters are the strings that mark the end of the generic info
    protected ResultSet logSplitters;
    //Each log has a unique name, cpcode, domain and logtype
    protected int cpcode;
    protected Database db;
    protected ResultSet liveFix;
    protected String logName,logType, domain, cdn, deliveryType, client, breaker;
    protected List<? extends LogLine> logLines;
    protected List<String> stringLines;
    protected static int lineCount = 0;
    protected static int errorCount = 0;
    public Log(String logname,String logType,ResultSet logDetails, ResultSet logSplitters, ResultSet liveFix, Database db) throws SQLException,IOException,ParseException,Exception{

        this.logName = logname;
        this.logType = logType;
        this.liveFix = liveFix;
        this.db = db;
        this.logSplitters = logSplitters;
        this.cpcode = logDetails.getInt("cpcode");
        this.domain = logDetails.getString("domain");
        this.cdn = logDetails.getString("cdn");
        this.deliveryType = logDetails.getString("delivery_type");
        this.client = logDetails.getString("client");
        //Defines what the line items are split by - usually tab but sometimes space.
        this.breaker = logDetails.getString("split_characters");
        System.out.println("START: Log read....");
        readLog();
    }

    public static void addLine() {
        lineCount++;
    }

    public static int getLine() {
        return lineCount;
    }

    protected String getName() { return logName;}

    public static void addError() {
        errorCount++;
    }

    /**
     * This handles the reading in of the log. It first converts it into a string array for each line, then generates the correct logline objects before converting it to a list.
     * @throws Exception A general exception is used as we are keen to stop the processing of the file.
     */
    protected void readLog() throws Exception{
        lineCount = 0;
        errorCount = 0;
        stringLines = LogReader.OpenReader(logName);
         //Convert the strings into objects of type logline after checking that they aren't header lines
        this.logLines = stringLines.stream().
                filter(x -> (!(isHeader(x)))) // Make sure it is not a header line
                    .map(y -> LogLineFactory.makeLogLine(this, y, breaker, cpcode, logSplitters, logType)) // Create the log line object
                        .collect(Collectors.toList());

        System.out.println("COMPLETE: Log read....");

    }

    /**
     * After the log file has been processed, to enable bulk inserts it is neccessary to make sure that each log line has the same number of items in it's outputs.
     * This method handles checking whether they have been set and then setting them to empty if needs be.
     * It also enters in the log level information for each line such as CDN and cpcode.
     * @param outputs The data structure used to keep information on each log line
     */
    protected void addLogFields(Map<String,String> outputs){
        outputs.put("cdn",cdn);
        outputs.put("cpcode",Integer.toString(cpcode));
        outputs.put("delivery_method", deliveryType);
        outputs.put("domain",domain);
        String[] setOutputs = "path,file_ref,type,client,file_name,bandwidth,format,stream,segment_lines,directories,dir1,dir2,ggviewid,gghostid,ggcampaignid".split(",");
        for(String output : setOutputs) {
            if (!outputs.containsKey(output))  outputs.put(output,(output.equals("ggviewid")||output.equals("bandwidth")) ? "NULL"  : "");
        }
    }

    /**
     * After the log file has been processed, this inputs the log line into the database.
     * It generates the sql by reading in each log lines outputs data structure
     * @param line The actual log line which is being entered into the database
     */
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

        Database.getDB().bulkInsert(this, fields, inputs, Integer.parseInt(line.getOutputs().get("log_line")));
    }

    /**
     * Each logline has now been enter into the temporary database. If there are no errors then it should continue with the process of finalizing the log.
     * It will run geographic lookups on the ip address, move the data into the main logdata table and then generate summary data
     */
    protected void finalizeLog() {
        if (Log.errorCount == 0) {
            System.out.println("START: finalize log queries");
            Set<String> uniqueDates = new HashSet<String>();
            //Look up the unique dates in the log file. This is needed for creating efficient summary table queries.
           logLines.stream().forEach(line -> uniqueDates.add(line.getOutputs().get("date")));
            //Do geo lookup to put the country information in there
            db.operate(this, "update statistics.logdata_temp l inner join ip2location.ip_country c ON MBRCONTAINS(c.ip_poly, POINTFROMWKB(POINT(l.ip_number, 0))) SET l.country = c.country_name;");
            //Insert into main logdata
            db.operate(this, "INSERT INTO logdata SELECT * from logdata_temp;");
            //Delete from the days from summary path that contain this domains and dates
            db.operate(this, "DELETE FROM summary_path WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")");
            //Delete from the days from summary path that contain this domains and dates
            db.operate(this, "INSERT INTO summary_path(cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country,duration,throughput,hits,unique_viewers,date,type,gghostid,ggcampaignid) SELECT cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country, AVG(duration), sum(throughput), count(*), count(DISTINCT(ip_address)), date, type,gghostid,ggcampaignid from logdata  WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ") group by cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country,date,type,gghostid,ggcampaignid");
            //Delete from the days from summary client that contain this domains and dates
            db.operate(this, "DELETE FROM summary_client WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")");
            //Delete from the days from summary path that contain this domains and dates
            db.operate(this, "INSERT INTO summary_client(cdn,cpcode,domain,client,dir1,country,device,throughput,hits,unique_viewers,date,type,gghostid,ggcampaignid) SELECT cdn,cpcode,domain,client,dir1,country,device, sum(throughput), count(*), count(DISTINCT(ip_address)), date, type,gghostid,ggcampaignid from logdata   WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")group by cdn,cpcode,domain,client,dir1,country,date,type,gghostid,ggcampaignid ");

            //Now record that the log has been successfully processed
            db.insert(this,"INSERT into processed_logs (logname, line_count) VALUES ('" + this.getName() + "'," + Log.getLine() + ")");
            System.out.println("END: finalize log queries");

        }

        // truncate temporary table
        db.operate(this, "TRUNCATE TABLE logdata_temp");

    }
    /**
     * When generating the SQL query to insert into the log table we need to check whether the items are numbers of strings
     * @param key the name of the field
     * @param item the value of the field
     */
    public static String fieldType(String key,Object item) {

        switch(key) {
            case "ip_number":
            case "scbytes":
            case "status":
            case "bandwidth":
            case "log_line":
            case "ggviewid":
                return (item == null) ? "" : item.toString();
            default:
                return (item == null) ? "''" : "'"+item.toString() + "'";
        }

    }

    /**
     * Checks whether the logline is a header line or not. We won't process the line if it is a header.
     * @param line The actual log line which is being entered into the database
     */
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

    /**
     * This converts a set of dates into a where condition of an sql query listing the dates
     * @param dates A set of dates with all the unique dates in the log file
     * @return dateStr part of an SQL query listing the unique dates that the file contains
     */
    protected String getUniqueDates(Set<String> dates) {
        String dateStr = "";
        String[] dateArr = dates.toArray(new String[dates.size()]);
        for (int i = 0; i < dateArr.length; i++) {
            dateStr += (i == (dateArr.length) - 1) ? "date = '" + dateArr[i] + "'" : "date = '" + dateArr[i] + "' or ";
        }
        return dateStr;
    }
}
