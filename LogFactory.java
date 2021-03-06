package code;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This handles the generation of log objects. It uses reflection to create classes based upon the the class that the database table specifies that this cpcode should use.

 */
public class LogFactory {
    /**
     * This method handles generating the log objects.
     * @param logname The name of the log file
     * @param rsLogTypes The record set of all of the different cp codes and the classes they should use.
     * @param rsLogSplitters The record set of all of the different log splitters which are used for splitting urls
     * @param fixLive The record set for live fixes so that any live data can be attributed to the right clients.
     * @param db The database object
     * @param reader The object responsible for opening log files up
     * @param logger The error logger object
     * @param debug Whether this is in testing mode or not ( for unit tests). If it is in testing mode then nothing will be added to the database.
     * @return An object that is an instance of a sub class of log
     * @throws ClassNotFoundException If the class doesn't exist
     * @throws IllegalAccessException If we don't have permission to use a class/ method from here
     * @throws InstantiationException An error occurs when creating the class
     * @throws NoSuchMethodException The method doesn't exist
     * @throws InvocationTargetException There was a problem with the constructor
     * @throws SQLException An SQL error has occurred
     */
    public static Log makeLog(String logname, ResultSet rsLogTypes, ResultSet rsLogSplitters, ResultSet fixLive, Database db, LogReader reader, ErrorLog logger, Boolean debug) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException,SQLException {
        //Split the logname into an array and get the first item (which is the cpcode of the log)
        int cpcode = Integer.parseInt(logname.split("_")[0]);
        Boolean logFound = false;
        //Reset the recordset to beginning.
        rsLogTypes.beforeFirst();
        while (rsLogTypes.next()) {
            if (cpcode == rsLogTypes.getInt("cpcode")) {
                logFound = true;
                break;
            }
        }

        if (!logFound) {
            throw new IllegalArgumentException("This cpcode " + cpcode + " is not recorded in the log_types database.");
        } else {
            return (Log) Class.forName("code." + rsLogTypes.getString("log_type")).getConstructor(String.class,ResultSet.class, ResultSet.class, ResultSet.class,Database.class,LogReader.class,ErrorLog.class, Boolean.class).newInstance(logname, rsLogTypes,rsLogSplitters,fixLive,db,reader,logger, debug);
        }

    }
}
