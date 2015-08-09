package code;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public abstract class  Log {
    //Splitters are the strings that mark the end of the generic info
    protected ResultSet logSplitters;
    //Each log has a unique name, cpcode, domain and logtype
    protected int cpcode;
    protected String logName, domain, cdn, deliveryType, client, breaker;
    protected List<LogLine> logLines;
    private static int lineCount = 0;
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
    }

    protected void readLog() throws IOException{
        lineCount = 0;
        List<String> stringLines = LogReader.OpenReader(logName);
         //Convert the strings into objects of type logline

        this.logLines =  stringLines.stream().map(y -> new LogLine(y,breaker,cpcode,logSplitters)).collect(Collectors.toList());
    }

    public static void addLine() {
        lineCount++;
    }

    public static int getLine() {
        return lineCount;
    }



}
