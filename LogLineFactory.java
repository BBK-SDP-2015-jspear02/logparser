package code;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LogLineFactory {
    public static LogLine makeLogLine(Log log,String logline, String breaker, int cpcode, ResultSet splitters, String logLineType)  {
        try {
            return (LogLine) Class.forName("code." + logLineType + "Line").getConstructor(String.class, String.class, int.class, ResultSet.class).newInstance(logline, breaker, cpcode, splitters);
        }catch (ClassNotFoundException e) {
            System.out.println("cnf" + e.getMessage());
        } catch (IllegalAccessException e) {
            System.out.println("illegal access" + e.getMessage());
        } catch (InstantiationException e) {
            System.out.println("instantiation" + e.getMessage());
        } catch (NoSuchMethodException e) {
            System.out.println(e.getMessage());
        } catch (InvocationTargetException e) {
            System.out.println("invocation" + e.getMessage());
        }

        System.out.println("Problem with log " + log.getName() + " on line " + Log.lineCount + ". This log has not been processed.");
        //If it fails (it never should) - just attempt to process it as a normal logline
        return new LogLine(logline, breaker, cpcode, splitters);
    }
}
