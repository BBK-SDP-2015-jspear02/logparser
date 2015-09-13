package code;

import java.sql.ResultSet;

public class LogStream extends Log {
    public LogStream(String logname,String logType, ResultSet logDetails, ResultSet logSplitters, ResultSet liveFix, Database db) throws Exception {
        super(logname,logType,logDetails, logSplitters, liveFix, db);
    }
}
