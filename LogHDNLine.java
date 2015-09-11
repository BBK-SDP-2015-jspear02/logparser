package code;


import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
/**
 * This is the HDN Log line class which inherits from the main log line class.
 */
public class LogHDNLine extends LogLine{
    public LogHDNLine(Log log,String logline, String breaker, int cpcode, ResultSet splitters) throws URISyntaxException,SQLException{
        //Add one item to the log line
        super(log,logline,breaker,cpcode,splitters);
        processLine();
        Url.urlSplitHDN(outputs,splitters);
    }

    /**
     * This will add in data from a segment file to a master playlist
     * @param line The segment line which is being added to the master
     */
    public void addSegment(LogHDNLine line) {
        // adding segment information to the logline
        outputs.put("throughput", Double.toString(Double.parseDouble(outputs.get("throughput")) + Double.parseDouble(line.getOutputs().get("throughput"))));
        outputs.put("scbytes", Integer.toString(Integer.parseInt(outputs.get("scbytes")) + Integer.parseInt(line.getOutputs().get("scbytes"))));
        outputs.put("segment_lines", (outputs.containsKey("segment_lines")) ? outputs.get("segment_lines") + "," + line.getOutputs().get("log_line") : line.getOutputs().get("log_line"));

        //As long as it's not another reference file add to the duration and segment count.
        if(!(line.getOutputs().get("file_type").equals("m3u8"))) {
            //Add 10 as each 'chunk' is 10 seconds long
            outputs.put("duration", Integer.toString(Integer.parseInt(outputs.get("duration")) + 10));
            outputs.put("segment_count", Integer.toString(Integer.parseInt(outputs.get("segment_count")) + 1));
            int segmentBandwidth = Integer.parseInt(line.getOutputs().get("bandwidth"));
            //If bandwidth is already set then run a rolling average - otherwise set bandwidth to the first segment.
            outputs.put("bandwidth", Integer.toString((outputs.containsKey("bandwidth")) ? (Integer.parseInt(outputs.get("bandwidth")) + segmentBandwidth) / 2 : segmentBandwidth));
        }
    }
}
