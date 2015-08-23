package code;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;

//Generates log objects. Different log items generate different type of log items.
public class LogFactory {
    public static Log makeLog(String logname, ResultSet rsLogTypes, ResultSet rsLogSplitters, ResultSet fixLive) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException,SQLException {
        //Split the logname into an array and get the first item (which is the cpcode of the log)
        int cpcode = Integer.parseInt(logname.split("_")[0]);
        Boolean logFound = false;
        //First query the DB to find out what class should be used to analyse the logs
        rsLogTypes.beforeFirst();
        while (rsLogTypes.next()) {
            System.out.println(rsLogTypes.getInt("cpcode"));
            if (cpcode == rsLogTypes.getInt("cpcode")) {
                logFound = true;
                break;
            }
        }
        //Reset back to the beginning

        if (!logFound) {
            throw new IllegalArgumentException("This cpcode " + cpcode + " is not recorded in the log_types database.");
        } else {
            return (Log) Class.forName("code." + rsLogTypes.getString("log_type")).getConstructor(String.class,String.class,ResultSet.class, ResultSet.class, ResultSet.class).newInstance(logname,rsLogTypes.getString("log_type"), rsLogTypes,rsLogSplitters,fixLive);
        }

    }
}
