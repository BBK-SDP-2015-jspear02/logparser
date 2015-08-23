package code;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

public class LogReader {

    //Open the file and return all the lines in a list of strings
    public static List<String> OpenReader(String logname) throws IOException {
        FileReader reader = new FileReader(RunIt.unprocessedLogs + logname );

        try (BufferedReader fileText = new BufferedReader(reader)) {
            return fileText.lines()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
