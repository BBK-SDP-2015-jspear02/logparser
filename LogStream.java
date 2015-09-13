package code;

import java.sql.ResultSet;

public class LogStream extends Log {
    public LogStream(String logname, ResultSet logDetails, ResultSet logSplitters, ResultSet liveFix, Database db) throws Exception {
        super(logname,logDetails, logSplitters, liveFix, db);
    }
}
