package code;
import java.sql.*;

public class Database {
    private Connection conn;
    private static final String url = "jdbc:mysql://localhost/statistics";
    private static final String username = "root";
    private static final String password = "groovy";
    public Database() {
        System.out.println("Connecting database...");
        connect();
    }
    private void connect() {
        try {
            conn = DriverManager.getConnection(url, username, password);
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
}
