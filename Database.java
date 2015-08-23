package code;
import java.sql.*;

public class Database {
    private Connection conn;
    private static final String url = "jdbc:mysql://localhost/statistics";
    private static final String username = "root";
    private static final String password = "groovy";
    private String fields;
    private String values;
    private int insertCount;
    public Database() {
        connect();
    }
    private void connect() {
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
    public ResultSet select(String sql) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            return rs;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot execute query!" + e.getMessage(), e);
         }

    }

    public void insert(Log log,String sql) {
        try {
            Statement stmt = conn.createStatement();
            System.out.println(sql);
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            RunIt.logger.writeError(log.getName(),0,e.getMessage());
        }

    }

    public void bulkInsert(Log log, String fields, String values, int logLine) {
        if ((insertCount == 500)|| (logLine == Log.getLine())) {
            //Insert every 200 rows or when the log file is finished.
            this.values += values;
            //Do a bulk insert every 1000 rows
            insert(log,this.fields + this.values);
            insertCount = 0;
            this.values = "";
        } else {
            this.values += values + ",";
        }
        this.fields = fields;
        insertCount++;


    }

    public void operate(Log log,String sql) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            RunIt.logger.writeError(log.getName(),0,e.getMessage());
        }
    }

    public void insertError(String sql) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());

            //throw new IllegalStateException("Cannot execute query!" + e.getMessage(), e);
        }

    }
}
