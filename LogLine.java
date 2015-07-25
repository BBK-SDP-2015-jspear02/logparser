package code;

import java.util.Arrays;
import java.util.List;

public class LogLine {
    String logline;
    List<String> lineItems;
    public LogLine(String logline) {
        this.logline = logline;
    }
    private void splitLine(String breaker) {
        lineItems = Arrays.asList(logline.split(" "));
    }
}
