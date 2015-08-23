package code;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;

public class LogLineFactory {
    public static LogLine makeLogLine(Log log,String logline, String breaker, int cpcode, ResultSet splitters, String logLineType)  {
        try {
            System.out.println(Log.lineCount);
            return (LogLine) Class.forName("code." + logLineType + "Line").getConstructor(Log.class,String.class, String.class, int.class, ResultSet.class).newInstance(log,logline, breaker, cpcode, splitters);
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
        return new LogLine(log,logline, breaker, cpcode, splitters);
    }
}
