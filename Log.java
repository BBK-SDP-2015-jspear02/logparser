package code;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is the main Log file format. Other, more specific log file formats inherit from this but much of the processing of the log takes place here.
 */
public abstract class  Log {
    //Splitters are the strings that mark the end of the generic info
    protected ResultSet logSplitters;
    //Each log has a unique name, cpcode, domain and logtype
    protected int cpcode;
    protected boolean debug;
    protected Database db;
    protected ResultSet liveFix;
    protected ErrorLog logger;
    protected String logName,logType, domain, cdn, deliveryType, client, breaker;
    protected List<? extends LogLine> logLines;
    protected List<String> stringLines;
    protected LogReader reader;
    protected static int lineCount = 0;
    protected static int errorCount = 0;
    public Log(String logname,ResultSet logDetails, ResultSet logSplitters, ResultSet liveFix, Database db, LogReader reader, ErrorLog logger, Boolean debug) throws Exception{
        this.logName = logname;
        this.liveFix = liveFix;
        this.db = db;
        this.reader = reader;
        this.logger = logger;
        this.debug = debug;
        this.logSplitters = logSplitters;
        this.logType = logDetails.getString("log_type");;
        this.cpcode = logDetails.getInt("cpcode");
        this.domain = logDetails.getString("domain");
        this.cdn = logDetails.getString("cdn");
        this.deliveryType = logDetails.getString("delivery_type");
        this.client = logDetails.getString("client");
        //Defines what the line items are split by - usually tab but sometimes space.
        this.breaker = logDetails.getString("split_characters");
        //As a belt and braces approach we will truncate the temporary log table here
        db.operate(this, "TRUNCATE TABLE logdata_temp");


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
        System.out.println("START: Read log....");
        lineCount = 0;
        errorCount = 0;
        stringLines = reader.OpenReader(logName);
         //Convert the strings into objects of type logline after checking that they aren't header lines
        this.logLines = stringLines.stream().
                filter(x -> (!(isHeader(x)))) // Make sure it is not a header line
                    .map(y -> LogLineFactory.makeLogLine(this, y, breaker, logSplitters, logType,logger)) // Create the log line object
                        .collect(Collectors.toList());

        System.out.println("COMPLETE: Read log....");

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
    protected void insertToDB(LogLine line) throws SQLException{
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

        db.bulkInsert(this, fields, inputs, Integer.parseInt(line.getOutputs().get("log_line")));
    }
    /**
     * This takes care of entering each log line object from the logLines list in to the temporary table
     * Once finished it finalizes the log file.
     * IMPORTANT NOTE: If the debug boolean is set to true then no data will be entered in to the database. This is to allow for unit testing.
     */
    protected void insertAllLinesToTempDB() throws SQLException{
       if (!debug) {
           System.out.println("START: Insert lines to temp table.");

           for (LogLine line : logLines) {
               insertToDB(line);
           }
           System.out.println("COMPLETE: Insert lines to temp table.");
           //Now finalize the log
           finalizeLog();
       }
    }
    /**
     * Each logline has now been enter into the temporary database. If there are no errors then it should continue with the process of finalizing the log.
     * It will run geographic lookups on the ip address, move the data into the main logdata table and then generate summary data
     */
    protected void finalizeLog() throws SQLException{
        if (Log.errorCount == 0)  {
            Set<String> uniqueDates = new HashSet<String>();
            //Look up the unique dates in the log file. This is needed for creating efficient summary table queries.
            logLines.stream().forEach(line -> uniqueDates.add(line.getOutputs().get("date")));

            System.out.println("START: GeoQuery update.....");
            //Do geo lookup to put the country information in there
            db.operate(this, "update statistics.logdata_temp l inner join ip2location.ip_country c ON MBRCONTAINS(c.ip_poly, POINTFROMWKB(POINT(l.ip_number, 0))) SET l.country = c.country_name;");
            System.out.println("COMPLETE: GeoQuery update.");

            System.out.println("START: Move temp table to logdata");
            //Insert into main logdata
            db.operate(this, "INSERT INTO logdata SELECT * from logdata_temp;");
            System.out.println("COMPLETE: Move temp table to logdata");

            System.out.println("START: Build summary path");
            //Delete from the days from summary path that contain this domains and dates
            db.operate(this, "DELETE FROM summary_path WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")");
            //Delete from the days from summary path that contain this domains and dates
            db.operate(this, "INSERT INTO summary_path(cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country,duration,throughput,hits,unique_viewers,date,type,gghostid,ggcampaignid) SELECT cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country, AVG(duration), sum(throughput), count(*), count(DISTINCT(ip_address)), date, type,gghostid,ggcampaignid from logdata  WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ") group by cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country,date,type,gghostid,ggcampaignid");
            System.out.println("COMPLETE: Build summary path");

            System.out.println("START: Build summary client");
            //Delete from the days from summary client that contain this domains and dates
            db.operate(this, "DELETE FROM summary_client WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")");
            //Delete from the days from summary path that contain this domains and dates
            db.operate(this, "INSERT INTO summary_client(cdn,cpcode,domain,client,dir1,country,device,throughput,hits,unique_viewers,date,type,gghostid,ggcampaignid) SELECT cdn,cpcode,domain,client,dir1,country,device, sum(throughput), count(*), count(DISTINCT(ip_address)), date, type,gghostid,ggcampaignid from logdata   WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")group by cdn,cpcode,domain,client,dir1,country,date,type,gghostid,ggcampaignid ");
            System.out.println("COMPLETE: Build summary client");

            System.out.println("START: Insert log file into processed logs");
            //Now record that the log has been successfully processed
            db.insert(this,"INSERT into processed_logs (logname, line_count) VALUES ('" + this.getName() + "'," + Log.getLine() + ")");
            System.out.println("COMPLETE: Insert log file into processed logs");

        }
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
