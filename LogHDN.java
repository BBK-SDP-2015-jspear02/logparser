package code;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class LogHDN extends Log{
    public LogHDN(String logname, ResultSet logDetails, ResultSet logSplitters) throws SQLException,IOException{
        super(logname,logDetails, logSplitters);
    }
    protected void readLog() throws IOException{
        lineCount = 0;
        List<String> stringLines = LogReader.OpenReader(logName);
        //Convert the strings into objects of type logline after checking that they aren't header lines
        try {
            this.logLines = stringLines.stream().filter(x -> (!(super.isHeader(x)))).map(y -> new LogLine(y, breaker, cpcode, logSplitters)).collect(Collectors.toList());
        } catch (Exception ex) {
            System.out.println("Problem with log file " + logName + " on line " + lineCount + ".");
        }
    }
}
