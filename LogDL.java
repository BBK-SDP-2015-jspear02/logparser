package code;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;


public class LogDL extends Log{
    public LogDL(String logname, ResultSet logDetails, ResultSet logSplitters) throws SQLException,IOException {
        super(logname,logDetails, logSplitters);
    }

    protected void readLog() throws IOException{
        super.readLog();
    }
}
