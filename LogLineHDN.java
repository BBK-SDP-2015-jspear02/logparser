package code;

import java.sql.ResultSet;
import java.util.HashMap;

/**
 * Created by John on 15/08/2015.
 */
public class LogLineHDN extends LogLine{
    public LogLineHDN(String logline, String breaker, int cpcode, ResultSet splitters){
        //Add one item to the log line
        super(logline,breaker,cpcode,splitters);

    }
}
