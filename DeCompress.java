package code;


import java.io.IOException;

/**
 * This interface is responsible for controlling the decompression of log files of varying sorts.
 */
public interface DeCompress {
    /**
     * This method handles decompressing log files.
     * @param gzLog The location of the file which needs to be decompressed
     * @param log The location that the decompressed file will be put
     * @throws IOException If it is unable to find or read the file for decompression
     */
    public void unzip(String gzLog, String log) throws IOException;
}
