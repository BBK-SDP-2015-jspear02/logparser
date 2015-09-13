package code;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LogStreamLine extends LogLine {
    public LogStreamLine(Log log, String logline, String breaker,  ResultSet splitters) throws URISyntaxException, SQLException {
        //Add one item to the log line
        super(log, logline, breaker, splitters);
        processLine();
        Url.urlSplitLive(outputs, splitters);
    }
}