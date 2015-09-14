package code;
import java.sql.*;
/**
 * This class first is used to run any database operations. It is only instantiated once - on RunIt.
 */
public class Database {
    private Connection conn;
    private String fields;
    private String values;
    private boolean debug = false;
    private int insertCount;
    private static Database db;
    private Database(String url, String username, String password) {
        connect(url,username,password);
    }
    public static Database getInstance(String url,String username, String password){
        if (db == null) {
            db = new Database(url,username,password);
        }
        return db;
    }
    /**
     * Handles setting up the connection to the database.
     */
    private void connect(String url,String username, String password) {
        try {
            conn = DriverManager.getConnection(url, username, password);
            fields = "";
            values = "";
            insertCount = 0;
            System.out.println("Database connected!");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
    }

    /**
     * Any select queries are handled here.
     * @return rs - the result set containing the data that has been queried.
     */
    public ResultSet select(String sql) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            return rs;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot execute query!" + e.getMessage(), e);
         }

    }

    /**
     * Any select queries are handled here.
     * @param log The log file that is being read at the time of this insert
     * @param sql The actual sql statement which is being run on the insert
     */
    public void insert(Log log,String sql) throws SQLException{

            Statement stmt = conn.createStatement();
            if (debug) System.out.println(sql);
            stmt.executeUpdate(sql);

    }
    /**
     * Any select queries are handled here.
     * @param log The name of the file that is being read at the time of this error
     * @param sql The actual sql statement which is being run on the insert
     */
    public void insertError(String log,String sql) throws SQLException{

            Statement stmt = conn.createStatement();
             if (debug) System.out.println(sql);
            stmt.executeUpdate(sql);

    }
    /**
     * For speed, as inserting data one record at a time is very slow. This query builds up one large insert of x rows.
     * When it reaches x rows or the end of the log file it actually runs the query.
     * @param log The log file that is being read at the time of this insert
     * @param fields The fields to be inserted to part of the sql
     * @param values The values to be inserted part of the sql statement
     * @param logLine The line of the log file that is being processed.
     */
    public void bulkInsert(Log log, String fields, String values, int logLine) throws SQLException{
        if ((insertCount == 500)|| (logLine == Log.getLine())) {
            //Insert every 200 rows or when the log file is finished.
            this.values += values;

            insert(log,this.fields + this.values);
            insertCount = 0;
            this.values = "";
        } else {
            this.values += values + ",";
        }
        this.fields = fields;
        insertCount++;


    }
    /**
     * Used to run operations such as truncation, updates and moving data from one table to another
     * When it reaches x rows or the end of the log file it actually runs the query.
     * @param log The log file that is being read at the time of this insert
     * @param sql The sql statement that is being executed
     */
    public void operate(Log log,String sql) throws SQLException{
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);

    }
    /**
     * Used to run operations such as truncation, updates and moving data from one table to another when not using a log file
     * When it reaches x rows or the end of the log file it actually runs the query.
     * @param sql The sql statement that is being executed
     */
    public void operate(String sql) throws SQLException{
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
    }
}
