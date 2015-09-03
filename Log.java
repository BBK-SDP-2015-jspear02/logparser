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
        String[] setOutputs = "path,file_ref,type,file_name,directories,dir1,dir2,ggviewid,gghostid,ggcampaignid".split(",");
        for(String output : setOutputs) {
            if (!outputs.containsKey(output))  outputs.put(output,(output.equals("ggviewid")) ? "NULL"  : "");
        }
    }

    public static void addError() {
        errorCount++;
    }
    protected void getCountry(LogLine line) {
        ResultSet countryData = RunIt.db.select("SELECT c.countrylong from ip2location.ipcountry c where c.toIP >= l.ip_number order by c.toIP limit 1");

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
            Set<String> uniqueDates = new HashSet<String>();
           logLines.stream().forEach(line -> uniqueDates.add(line.getOutputs().get("date")));;
            //Do geo lookup to put the country information in there
            RunIt.db.operate(this,"update statistics.logdata_temp l inner join ip2location.ip_country c ON MBRCONTAINS(c.ip_poly, POINTFROMWKB(POINT(l.ip_number, 0))) SET l.country = c.country_name;");
            //Insert into main logdata
            RunIt.db.operate(this, "INSERT INTO logdata SELECT * from logdata_temp;");
            //Delete from the days from summary path that contain this domains and dates
            RunIt.db.operate(this, "DELETE FROM summary_path WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")");
            //Delete from the days from summary path that contain this domains and dates
            RunIt.db.operate(this, "INSERT INTO summary_path(cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country,duration,throughput,hits,unique_viewers,date,type,gghostid,ggcampaignid) SELECT cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country, AVG(duration), sum(throughput), count(*), count(DISTINCT(ip_address)), date, type,gghostid,ggcampaignid from logdata  WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ") group by cdn,cpcode,domain,client,directories,dir1,dir2,url,path,file_ref,file_name,file_type,delivery_method,format,country,date,type,gghostid,ggcampaignid");
            //Delete from the days from summary client that contain this domains and dates
            RunIt.db.operate(this, "DELETE FROM summary_client WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")");
            //Delete from the days from summary path that contain this domains and dates
            RunIt.db.operate(this, "INSERT INTO summary_client(cdn,cpcode,domain,client,dir1,country,device,throughput,hits,unique_viewers,date,type,gghostid,ggcampaignid) SELECT cdn,cpcode,domain,client,dir1,country,device, sum(throughput), count(*), count(DISTINCT(ip_address)), date, type,gghostid,ggcampaignid from logdata   WHERE cpcode = " + this.cpcode + " AND ("  + getUniqueDates(uniqueDates) + ")group by cdn,cpcode,domain,client,dir1,country,date,type,gghostid,ggcampaignid ");

            //Now record that the log has been succesfully processed
            RunIt.db.insert(this,"INSERT into processed_logs (logname, line_count) VALUES ('" + this.getName() + "'," + Log.getLine() + ")");
            //Now move the log file from unprocessed to processed

            try {

                Files.move(Paths.get(RunIt.unprocessedLogs + this.getName()), Paths.get(RunIt.processedLogs + this.getName()), REPLACE_EXISTING);

            } catch (Exception ex) {
                RunIt.logger.writeError(this.logName,0,ex.getMessage());
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
            case "ggviewid":
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

    protected String getUniqueDates(Set<String> dates) {
        String dateStr = "";
        String[] dateArr = dates.toArray(new String[dates.size()]);
        for (int i = 0; i < dateArr.length; i++) {
            dateStr += (i == (dateArr.length) - 1) ? "date = '" + dateArr[i] + "'" : "date = '" + dateArr[i] + "' or ";
        }
        return dateStr;
    }


}
