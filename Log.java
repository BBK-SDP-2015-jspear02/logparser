package code;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class  Log {
    //Splitters are the strings that mark the end of the generic info
    private String[] splitters;
    //Each log has a unique name, cdn, domain and logtype
    private String logName, domain, logType;
    private ArrayList<LogLine> logLines;
    public Log(String logname) {
        this.logName = logname;
        //First read the log file into the array
        readLog();
    }

    private void readLog() {
        try {
            List<String> logLines = LogReader.OpenReader(logName);
            //Loop through each log line
            logLines.stream().forEach(item -> System.out.println(item));

        }  catch(IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
