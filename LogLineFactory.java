package code;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;

/**
 * This class handles the generation of log line objects using reflection to dynamically create them.
 */
public class LogLineFactory {
    /**
     * This method handles generating the log objects.
     * @param log The log that this line belongs to
     * @param logline The text of the actual line
     * @param breaker the text that should be used to break one long line into an array of attributes
     * @param splitters The recordset which defines what url splitters to use for this cpcode
     * @param logLineType Defines which type of logline object this shoudl be.
     * @return The logline object
     */
    public static LogLine makeLogLine(Log log,String logline, String breaker, ResultSet splitters, String logLineType)  {
        try {
            return (LogLine) Class.forName("code." + logLineType + "Line").getConstructor(Log.class,String.class, String.class, ResultSet.class).newInstance(log,logline, breaker, splitters);
        }catch (ClassNotFoundException ex) {
            RunIt.logger.writeError(log.getName(), Log.getLine(), ex.getMessage());
        } catch (IllegalAccessException ex) {
            RunIt.logger.writeError(log.getName(), Log.getLine(), ex.getMessage());
        } catch (InstantiationException ex) {
            RunIt.logger.writeError(log.getName(), Log.getLine(), ex.getMessage());
        } catch (NoSuchMethodException ex) {
            RunIt.logger.writeError(log.getName(), Log.getLine(), ex.getMessage());
        } catch (InvocationTargetException ex) {
            RunIt.logger.writeError(log.getName(), Log.getLine(), ex.getCause().getMessage());
        }

        //If it fails (it never should) - just attempt to process it as a normal logline
        return new LogLine(log,logline, breaker, splitters);
    }
}
