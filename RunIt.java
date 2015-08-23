package code;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import static java.nio.file.StandardCopyOption.*;
public class RunIt {
    public static Database db = new Database();
    public static ErrorLog logger = new ErrorLog(db);
    public static final String unprocessedLogs = "C:\\Users\\John\\Documents\\00_UNI\\2014-15\\project\\logparser\\logs_unprocessed\\";
    public static final String processedLogs = "C:\\Users\\John\\Documents\\00_UNI\\2014-15\\project\\logparser\\logs_processed\\";
    public static void main(String[] args) {
        //Get the list of log types from the database
        ResultSet rsLogTypes = RunIt.db.select("SELECT * FROM log_types;");
        //Get the list of log url splitters from the database - order them by CPCODE and then length of the url
        ResultSet rsSplitters = RunIt.db.select("SELECT * FROM log_splitters ORDER BY cpcode, LENGTH(split) DESC;");
        //Get the list of log url splitters from the database - order them by CPCODE and then length of the url
        ResultSet rsLive = RunIt.db.select("SELECT * FROM live_fix;");
        File f = new File(unprocessedLogs);
        ArrayList<File> logs = new ArrayList<File>(Arrays.asList(f.listFiles()));
        for(File log : logs) {
            try {
                try {
                    Files.
                    Files.move((Path)RunIt.unprocessedLogs + log.getName(), RunIt.processedLogs + log.getName(), REPLACE_EXISTING);


                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                }
                LogFactory.makeLog(log.getName(), rsLogTypes, rsSplitters, rsLive);
                //Now move the log file from unprocessed to processed
            } catch (ClassNotFoundException e) {
                logger.writeError(log.getName(),0,e.getMessage());
            } catch (IllegalAccessException e) {
                logger.writeError(log.getName(), 0, e.getMessage());
            } catch (InstantiationException e) {
                logger.writeError(log.getName(), 0, e.getMessage());
            } catch (NoSuchMethodException e) {
                logger.writeError(log.getName(), 0, e.getMessage());
            } catch (InvocationTargetException e) {
                logger.writeError(log.getName(), 0, e.getMessage());
            } catch (IllegalArgumentException e) {
                logger.writeError(log.getName(), 0, e.getMessage());
            } catch (SQLException e) {
                logger.writeError(log.getName(),0,e.getMessage());
            }
        }

    }
}
