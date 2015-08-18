package code;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class RunIt {
    public static Database db = new Database();
    public static final String fileLocation = "C:\\Users\\John\\Documents\\00_UNI\\2014-15\\project\\logparser\\logs\\";
    public static void main(String[] args) {
        //Get the list of log types from the database
        ResultSet rsLogTypes = RunIt.db.select("SELECT * FROM log_types;");
        //Get the list of log url splitters from the database - order them by CPCODE and then length of the url
        ResultSet rsSplitters = RunIt.db.select("SELECT * FROM log_splitters ORDER BY cpcode, LENGTH(split) DESC;");
        //Get the list of log url splitters from the database - order them by CPCODE and then length of the url
        ResultSet rsLive = RunIt.db.select("SELECT * FROM live_fix;");
        File f = new File(fileLocation);
        ArrayList<File> logs = new ArrayList<File>(Arrays.asList(f.listFiles()));
            logs.stream().forEach(
                    item -> {
                        try {
                            LogFactory.makeLog(fileLocation, item.getName(), rsLogTypes, rsSplitters, rsLive);
                        } catch (ClassNotFoundException e) {
                            System.out.println("cnf" + e.getMessage());
                        } catch (IllegalAccessException e) {
                            System.out.println("illegal access" + e.getMessage());
                        } catch (InstantiationException e) {
                            System.out.println("instantiation" + e.getMessage());
                        } catch (NoSuchMethodException e) {
                            System.out.println(e.getMessage());
                        } catch (InvocationTargetException e) {
                            System.out.println("invocation" + e.getMessage());
                        } catch (IllegalArgumentException e) {
                            System.out.println("argument" + e.getMessage());
                        } catch (SQLException e) {
                            System.out.println("sql" + e.getMessage());
                        }
                    }
            );
    }
}
