package code;

public class ErrorLog {
    Database db;
    public ErrorLog(Database db) {
        this.db = db;
    }
    public void writeError(String logfile, int line,String message) {
        //Write the error
    }
}
