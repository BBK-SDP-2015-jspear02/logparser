package code;
import java.lang.reflect.InvocationTargetException;

//Generates log objects. Different log items generate different type of log items.
public class LogFactory {
    public static Log makeLog(String logname) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {

        Log log = (Log) Class.forName("code.LogHDN").getConstructor(String.class).newInstance(logname);

        return log;
    }
}
