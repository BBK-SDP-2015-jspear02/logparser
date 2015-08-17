package code;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogHDN extends Log{
    public LogHDN(String logname,String logType, ResultSet logDetails, ResultSet logSplitters,ResultSet liveFix) throws SQLException,IOException{
        super(logname,logType,logDetails, logSplitters,liveFix);
    }
    protected void readLog() throws IOException{
        super.readLog();
        analyseLog();
    }

    protected void analyseLog() {
        //Now all of the loglines have been created we need to strip out the partial requests to process them as individual lines
        //1. Move all the log lines with 206 status to a separate array

        List<LogHDNLine> masterHits = this.logLines.stream().filter(x -> playlistCheck(x.getOutputs().get("file_ref"))).map(x -> (LogHDNLine) x).collect(Collectors.toList());
        List<LogHDNLine> segmentHits = this.logLines.stream().filter(x -> !(playlistCheck(x.getOutputs().get("file_ref")))).map(x -> (LogHDNLine) x).collect(Collectors.toList());
       // masterHits.stream().forEach(x -> System.out.println(x.getOutputs().get("url")));
        Collections.reverse(masterHits);
        List<LogHDNLine> reversedHits = Collections.sort(masterHits,Collections.reverseOrder());
        //First create a map of all the distinct url_ipadrress_ua combinations. This is an original hit.
        Map<String,LogHDNLine> sessionLines = new HashMap<>();
        for(LogHDNLine line : Collections.reverse(masterHits)) {
            //Now loop through them. If they are already set then add to the original. Otherwise put it in there.
            String uaIpUrl = line.getOutputs().get("user_agent") + "_" + line.getOutputs().get("ip_address") + "_" + line.getOutputs().get("url");
            if (!(sessionLines.containsKey(uaIpUrl))){
                sessionLines.put(uaIpUrl,line);
            } else {
                //Add segment,throughput,duration to original item
                sessionLines.get(uaIpUrl).addSegment(line);
            }
        }
        //Now that's finished. Add the lines back in to the fullHits List
        sessionLines.forEach((key,logline)-> masterHits.add(logline));
        //Now stick them in the database!
        masterHits.stream().forEach(x-> insertToDB(x));
**/
    }

    private boolean playlistCheck(String fileref){
        return ((fileref.equals("master.m3u8"))||(fileref.equals("manifest.f4m"))) ? true : false;
    }
}
