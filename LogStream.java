package code;

import java.sql.ResultSet;

public class LogStream extends Log {
    public LogStream(String logname, ResultSet logDetails, ResultSet logSplitters, ResultSet liveFix, Database db, LogReader reader, ErrorLog logger, Boolean debug) throws Exception {
        super(logname,logDetails, logSplitters, liveFix, db, reader, logger, debug);
    }
}
