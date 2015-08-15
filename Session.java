package code;

/**
 * Created by John on 22/07/2015.
 */
public class Session {
    private String startTime,ipAddress,userAgent,url;
    private LogLine logLine;
    public Session(String startTime, String ipAddress,String userAgent,String url, LogLine logLine) {
        this.startTime = startTime;
        this.ipAddress = ipAddress;
        this.userAgent = userAgent;
        this.url = url;
        this.logLine = logLine;
    }
}
