package code;

import org.apache.commons.lang.StringEscapeUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LogDLLine extends LogLine{
    public LogDLLine(Log log,String logline, String breaker, int cpcode, ResultSet splitters){
        //Add one item to the log line
        super(log,logline,breaker,cpcode,splitters);
        //Now process this line
        processLine();
    }

    protected void urlSplit(){
        super.urlSplit();
    }
}
