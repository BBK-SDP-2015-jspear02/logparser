package code;
/**
 * This class first is used to handle any errors thrown during the log processing session
 * Any errors in an individual log file should be thrown back here, where they are entered into the logger.
 * It is a singleton, instantiated once.
 */
public class ErrorLog {
    private Database db;
    public ErrorLog(Database db) {
        this.db = db;
    }
    /**
     * This handles the processing of any error that couldn't be passed up the chain because of exception handling limitation with stream.
     * @param log The log file that it is currently processing
     * @param line The line that is causing the error
     * @param message The error message
     */
    public void writeError(Log log, int line,String message) {
        Log.addError();
        db.insert(log, "INSERT INTO error_log (logfile,line,message) VALUES ('" + log.getName() + "','" + line + "','" + checkMessage(message) + "');");
    }

    /**
     * This handles the processing of any error that has been passed up the chain back to the running class.
     * @param log The name of the log file that it is currently processing
     * @param line The line that is causing the error
     * @param message The error message
     */
    public void writeError(String log, int line,String message) {
        Log.addError();
        db.insertError(log, "INSERT INTO error_log (logfile,line,message) VALUES ('" + log + "','" + line + "','" + checkMessage(message) + "');");
    }

    /**
     * This handles the processing of any error that was generated without a log file.
     * @param message The error message
     */
    public void writeError(String message) {
        db.insertError("No log", "INSERT INTO error_log (logfile,line,message) VALUES ('No log','0','" + checkMessage(message) + "');");
    }

    /**
     * This sanitizes any strings in the message to make sure they will be entered in to the database
     * @param message The error message
     */
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
