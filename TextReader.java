package code;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class handles the i/o process of reading in the log files
 */
public class TextReader implements LogReader{
    private String fileLocation;
    public TextReader(String fileLocation ){
        this.fileLocation = fileLocation;
    };
    /**
     * Reads in the file and returns a list of text lines
     * @param logname The name of the log file which is being created
     * @return List of log lines as text
     * @throws IOException If there is an error when attempting to read the file
     */
    public List<String> OpenReader(String logname) throws IOException {
        FileReader reader = new FileReader(fileLocation + logname );

        try (BufferedReader fileText = new BufferedReader(reader)) {
            return fileText.lines()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
