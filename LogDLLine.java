package code;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
/**
 * This is the DL Log line class which inherits from the main log line class.
 */
public class LogDLLine extends LogLine{
    public LogDLLine(Log log,String logline, String breaker, int cpcode, ResultSet splitters)throws URISyntaxException,SQLException{
        //Add one item to the log line
        super(log,logline,breaker,cpcode,splitters);
        //Now process this line
        processLine();
        //Now process the url
        Url.urlSplit(outputs, splitters);
    }

}
