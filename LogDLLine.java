package code;

import org.apache.commons.lang.StringEscapeUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LogDLLine extends LogLine{
    public LogDLLine(String logline, String breaker, int cpcode, ResultSet splitters,ResultSet liveFix){
        //Add one item to the log line
        super(logline,breaker,cpcode,splitters,liveFix);
        //Now process this line
        processLine();
    }

    protected void urlSplit(){
        super.urlSplit();
    }
}
