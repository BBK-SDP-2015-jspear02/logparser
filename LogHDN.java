package code;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LogHDN extends Log{
    public LogHDN(String logname, ResultSet logDetails, ResultSet logSplitters) throws SQLException,IOException{
        super(logname,logDetails, logSplitters);
    }
}
