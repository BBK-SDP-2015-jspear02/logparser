package code;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class DeCompress {

    public static void unzip(String gzLog, String log) throws IOException{

        byte[] buffer = new byte[1024];
        FileInputStream fileIn = new FileInputStream(gzLog);
        GZIPInputStream logGzip = new GZIPInputStream(fileIn);
        FileOutputStream logOutput = new FileOutputStream(log);
        int bytes_read;
        System.out.println("START : Decompressing log file " + gzLog);
        while ((bytes_read = logGzip.read(buffer)) > 0) {
           logOutput.write(buffer, 0, bytes_read);
        }
        logGzip.close();
        logOutput.close();
        System.out.println("COMPLETE : Decompressing log file " + gzLog);
    }
}

