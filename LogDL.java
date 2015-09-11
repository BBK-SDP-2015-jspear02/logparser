package code;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is the Download Log format, it is a child of the Log class.
 */
public class LogDL extends Log{
    public LogDL(String logname,String logType, ResultSet logDetails, ResultSet logSplitters, ResultSet liveFix) throws Exception {
        super(logname,logType,logDetails, logSplitters, liveFix);
        analyseLog();
    }

    protected void readLog() throws Exception{
        super.readLog();
    }
    /**
     * The download logs also feature partial requests so some form of analysis is neccessary. This takes care of the analysis, attributing partial hits to full hits.
     */
    protected void analyseLog() {
        //Now all of the loglines have been created we need to strip out the partial requests to process them as individual lines
        //1. Move all the log lines with 206 status to a seperate array

            List<LogDLLine> partialHits = this.logLines.stream().filter(x -> x.getOutputs().get("status").equals("206")).map(x -> (LogDLLine) x).collect(Collectors.toList());
            List<LogDLLine> fullHits = this.logLines.stream().filter(x -> !(x.getOutputs().get("status").equals("206"))).map(x -> (LogDLLine) x).collect(Collectors.toList());

            //First create a map of all the distinct url_ipadrress_ua combinations. This is an original hit.
            Map<String,LogDLLine> sessionLines = new HashMap<>();
            for(LogDLLine line : partialHits) {
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
        sessionLines.forEach((key,logline)-> fullHits.add(logline));
        //Now stick them in the database!
        fullHits.stream().forEach(x-> insertToDB(x));
        //Now finalize the log!
        finalizeLog();

    }

}
