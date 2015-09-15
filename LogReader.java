package code;

import java.io.IOException;
import java.util.List;
/**
 * This interface handles the i/o process of reading in the log files of various sorts
 */
public interface LogReader {
    /**
     * Reads in the file and returns a list of text lines
     * @param logname The name of the log file which is being created
     * @return List of log lines as text
     * @throws IOException If there is an error when attempting to read the file
     */
    public List<String> OpenReader(String logname) throws IOException;
}
