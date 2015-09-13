package code;

import java.io.IOException;
import java.util.List;

public interface LogReader {
    public List<String> OpenReader(String logname) throws IOException;
}
