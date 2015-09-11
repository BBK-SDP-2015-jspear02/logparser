package code;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class first is used to run the whole process of unpacking, reading and inserting the log files into the database.
 * Any errors in an individual log file should be thrown back here, where they are entered into the logger.
 */
public class RunIt {
    public static Database db = new Database();
    public static ErrorLog logger = new ErrorLog(db);
    public static final String unprocessedLogs = "C:\\Users\\John\\Documents\\00_UNI\\2014-15\\project\\logparser\\logs_unprocessed\\";
    public static final String processedLogs = "C:\\Users\\John\\Documents\\00_UNI\\2014-15\\project\\logparser\\logs_processed\\";

    /**
     * Before the log reading process starts a series of queries are run which return :
     * rsLogTypes : The different log types, instructing the LogFactory and LogLineFactory classes which classes to create
     * rsSplitter : Bring back a list of splitters for each cpcode so that the url's can be broken up and assigned to right clients
     * rsLive: The list of live publishing points so that live log data can be assigned to clients.
     * @param args Not used in this case
     */
    public static void main(String[] args) {
        //Get the list of log types from the database
        ResultSet rsLogTypes = RunIt.db.select("SELECT * FROM log_types;");
        //Get the list of log url splitters from the database - order them by CPCODE and then length of the url
        ResultSet rsSplitters = RunIt.db.select("SELECT * FROM log_splitters ORDER BY cpcode, LENGTH(split) DESC;");
        //Get the list of log url splitters from the database - order them by CPCODE and then length of the url
        ResultSet rsLive = RunIt.db.select("SELECT * FROM live_fix;");
        //Clear the temporary table just in case there is any data left in there
        db.operate("TRUNCATE table logdata_temp;");
        File f = new File(unprocessedLogs);
        ArrayList<File> logs = new ArrayList<File>(Arrays.asList(f.listFiles()));
        //Loop through each log file in the directory and process it.
        for(File log : logs) {
            try {
                LogFactory.makeLog(log.getName(), rsLogTypes, rsSplitters, rsLive);
            } catch (ClassNotFoundException e) {
                //The class that was suggested by rsLogTypes for this cpcode doesn't exist
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
                //An SQL Exception has occurred when reading a recordset.
                logger.writeError(log.getName(),0,e.getMessage());
            }
        }

    }
}
