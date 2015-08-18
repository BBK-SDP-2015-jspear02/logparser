package code;

import org.apache.commons.lang.StringEscapeUtils;

import java.sql.ResultSet;

public class LogHDNLine extends LogLine{
    public LogHDNLine(String logline, String breaker, int cpcode, ResultSet splitters){
        //Add one item to the log line
        super(logline,breaker,cpcode,splitters);
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

            if((outputs.get("file_ref").indexOf(".m3u8")== -1 ) && (outputs.get("file_ref").indexOf("f4m")== -1 )) {
                //Not a playlist file
                String[] itemSplit = outputs.get("file_ref").split("_");
                //Get the bandwidth being watched in kbps
                outputs.put("bandwidth", (outputs.get("file_ref").indexOf("-Frag") != -1) ? itemSplit[0] : itemSplit[1] );
                outputs.put("segment_count", "1");
                outputs.put("duration", "10");
            } else {
                outputs.put("segment_count", "0");
                outputs.put("duration", "0");
            }
        } else {
            //Request for the crossdomain file. Not charged to a client but attributed to GG
            outputs.put("client", "GG");
            outputs.put("file_ref", basicInfo[0]);
        }
        super.fileSplit();
   }
    public void addSegment(LogHDNLine line) {
        // adding segment information to the logline
        outputs.put("throughput", Double.toString(Double.parseDouble(outputs.get("throughput")) + Double.parseDouble(line.getOutputs().get("throughput"))));
        outputs.put("scbytes", Integer.toString(Integer.parseInt(outputs.get("scbytes")) + Integer.parseInt(line.getOutputs().get("scbytes"))));
        //As long as it's not another reference file add to the duration and segment count.
        if(!(line.getOutputs().get("file_type").equals("m3u8"))) {
            //Add 10 as each 'chunk' is 10 seconds long
            outputs.put("duration", Integer.toString(Integer.parseInt(outputs.get("duration")) + 10));
            outputs.put("segment_count", Integer.toString(Integer.parseInt(outputs.get("segment_count")) + 1));
            int segmentBandwidth = Integer.parseInt(line.getOutputs().get("bandwidth"));
            //If bandwidth is already set then run a rolling average - otherwise set bandwidth to the first segment.
            outputs.put("bandwidth", Integer.toString((outputs.containsKey("bandwidth")) ? (Integer.parseInt(outputs.get("bandwidth")) + segmentBandwidth) / 2 : segmentBandwidth));
            System.out.println(outputs.get("bandwidth"));
        }
    }
}
