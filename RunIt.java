package code;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * This class first is used to run the whole process of unpacking, reading and inserting the log files into the database.
 * Any errors in an individual log file should be thrown back here, where they are entered into the logger.
 */
public class RunIt {
    private static final String dbURL = "jdbc:mysql://localhost/statistics";
    private static final String dbUsername = "root";
    private static final String dbPassword = "groovy";
    private static final String basePath = "C:\\Users\\John\\Documents\\00_UNI\\2014-15\\project\\logparser\\";
    public static final String gzmLogs = basePath + "logs_gzm"; //location of unprocessed, compressed log files
    public static final String unprocessedLogs = basePath +"logs_unprocessed\\"; //location of unprocessed, decompressed log files
    public static final String processedLogs = basePath + "logs_processed\\"; //location to move processed log files
    private static Database db;
    public static ErrorLog logger;
    /**
     * Before the log reading process starts a series of queries are run which return :
     * rsLogTypes : The different log types, instructing the LogFactory and LogLineFactory classes which classes to create
     * rsSplitter : Bring back a list of splitters for each cpcode so that the url's can be broken up and assigned to right clients
     * rsLive: The list of live publishing points so that live log data can be assigned to clients.
     * @param args Not used in this case
     */
    public static void main(String[] args) {
        db = Database.getInstance(dbURL,dbUsername,dbPassword);
        logger = ErrorLog.getInstance(db);
        //Get the list of log types from the database
        ResultSet rsLogTypes = db.select("SELECT * FROM log_types;");
        //Get the list of log url splitters from the database - order them by CPCODE and then length of the url
        ResultSet rsSplitters = db.select("SELECT * FROM log_splitters ORDER BY cpcode, LENGTH(split) DESC;");
        //Get the list of log url splitters from the database - order them by CPCODE and then length of the url
        ResultSet rsLive = db.select("SELECT * FROM live_fix;");
        //Get the list of processed logs so we never re-process a log by accident
        ResultSet rsProcessed = db.select("SELECT * FROM processed_logs;");

        File f = new File(gzmLogs);
        ArrayList<File> logs = new ArrayList<File>(Arrays.asList(f.listFiles()));
        //Loop through each log file in the directory and process it.
        for(File log : logs) {
            try {
                //Check to see whether the log is already in the list of processed logs. If it is then it shouldn't be processed.
                while (rsProcessed.next()) {
                    if (rsProcessed.getString("logname").equals(log.getName())) {
                        throw new IllegalArgumentException("The log file " + log.getName() + " has already been processed. Being ignored.");
                    }
                }
                //Reset the result set
                rsProcessed.beforeFirst();

                //Decompress log file, and place it in the unprocessed logs folder
                DeCompress inflate = new DeGZ();
                inflate.unzip(log.toString(), log.toString().replace(gzmLogs, unprocessedLogs));
                //Now process the log file
                LogFactory.makeLog(log.getName(), rsLogTypes, rsSplitters, rsLive, db);

                //Now move the log file from unprocessed to processed
                try {
                    System.out.println("START: move log file to processed");
                    //Move the compressed file to the processed logs to keep as archive
                    Files.move(Paths.get(RunIt.gzmLogs + log.getName()), Paths.get(RunIt.processedLogs + log.getName()), REPLACE_EXISTING);
                    //Delete the uncompressed file.
                    Files.delete(Paths.get(RunIt.unprocessedLogs + log.getName()));
                    System.out.println("COMPLETE: move log file to processed");
                } catch (Exception ex) {
                    RunIt.logger.writeError(log.getName(),0,ex.getMessage());
                }

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
            } catch (IOException e) {
                logger.writeError(log.getName(), 0, e.getMessage());
            } catch (SQLException e) {
                //An SQL Exception has occurred when reading a recordset.
                logger.writeError(log.getName(),0,e.getMessage());
            }
        }

    }
}
