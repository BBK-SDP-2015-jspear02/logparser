package code;

public class ErrorLog {
    private Database db;
    public ErrorLog(Database db) {
        this.db = db;
    }
    public void writeError(String logfile, int line,String message) {
        Log.addError();
        db.insertError("INSERT INTO error_log (logfile,line,message) VALUES ('" + logfile + "','" + line + "','" + checkMessage(message) + "');");
    }

    public void writeError(LogLine logLine, String message) {

    }

    private String checkMessage(String message){
        String rtnString = "";
        if (message == null) {
            rtnString = "No message given.";
        } else {
            rtnString = message.replace("'", "\\'");
        }
        return rtnString;
    }
}
