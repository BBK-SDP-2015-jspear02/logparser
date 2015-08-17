package code;

import org.apache.commons.lang.StringEscapeUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class LogHDNLine extends LogLine{
    public LogHDNLine(String logline, String breaker, int cpcode, ResultSet splitters,ResultSet liveFix){
        //Add one item to the log line
        super(logline,breaker,cpcode,splitters,liveFix);
        processLine();
    }

    protected void urlSplit(){
        String[] basicInfo = super.urlSplitBasic();

        //Create the directory array
        String[] dirArr = basicInfo[0].split("/");
        outputs.put("segment_count", "0");
        outputs.put("duration", "0");

        //HDN follows a special pattern. If the first directory is I then it is for HLS otherwise it is HDS
        if (dirArr.length > 1) {
            outputs.put("format", (dirArr[0].equals("i")) ? "HLS" : "HDS");
            outputs.put("path", dirArr[1]);
            outputs.put("file_ref", dirArr[2]);

            if((outputs.get("file_ref").indexOf(".m3u8")!= -1 ) && (outputs.get("file_ref").indexOf("f4m")!= -1 )) {
                //Not a playlist file
                String[] itemSplit = outputs.get("fileref").split("_");
                //Get the bandwidth being watched in kbps
                outputs.put("bandwidth", (outputs.get("file_ref").indexOf("-Frag") != -1) ? itemSplit[0] : itemSplit[1] );
                outputs.put("segment_count", "1");
                outputs.put("duration", "10");
            }

        } else {
            //Request for the crossdomain file. Not charged to a client
            outputs.put("client", "GG");
            outputs.put("file_ref", basicInfo[0]);
        }
        super.fileSplit();
   }
}
