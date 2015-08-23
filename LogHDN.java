package code;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class LogHDN extends Log{
    protected List<LogHDNLine> orphanLines;
    private double tput;
    public LogHDN(String logname,String logType, ResultSet logDetails, ResultSet logSplitters,ResultSet liveFix) throws SQLException,IOException,ParseException{
        super(logname,logType,logDetails, logSplitters,liveFix);
        orphanLines = new LinkedList<LogHDNLine>();
        tput = 0;
    }
    protected void readLog() throws IOException,SQLException,ParseException{
        lineCount = 0;
        errorCount = 0;
        stringLines = LogReader.OpenReader(logName);
        //Convert the strings into objects of type logline after checking that they aren't header lines
        try {
            this.logLines = stringLines.stream().
                    filter(x -> (!(isHeader(x))))
                    .map(y -> LogLineFactory.makeLogLine(this, y, breaker, cpcode, logSplitters, logType))
                    .filter(line -> (playlistCheck(line.getOutputs().get("file_ref")) || crossDomainCheck(line.getOutputs().get("file_ref"))))
                    .collect(Collectors.toList());

        } catch (Exception ex) {
            RunIt.logger.writeError(logName,getLine(),ex.getMessage());
        }
        analyseLog();
    }

    protected void analyseLog() throws SQLException,ParseException{
        //All of the playlist lines have been created. Now we need to go through the segments again, adding them to the right playlist.

        List<LogHDNLine> masterHits = this.logLines.stream()
                    .filter(x -> playlistCheck(x.getOutputs().get("file_ref")))
                         .map(line -> fixLive(line))
                            .map(x -> (LogHDNLine) x)
                                .collect(Collectors.toList());

        List<LogHDNLine> crossDomainHits = this.logLines.stream().filter(x -> (x.getOutputs().get("file_ref").equals("crossdomain.xml"))).map(x -> (LogHDNLine) x).collect(Collectors.toList());

        //The items are in time order. We need to reverse them.
        Collections.reverse(masterHits);

        //Collections.reverse(stringLines);

        //Reset log line count as we are reading again
        lineCount = 0;
        //Now read in the segment lines one by one - making sure not to keep any references to them.
        try {

            for (String line : stringLines) {
                if (!isHeader(line)) {
                    LogLine segmentLine = LogLineFactory.makeLogLine(this,line, breaker,cpcode,logSplitters,logType);

                    if (!(playlistCheck(segmentLine.getOutputs().get("file_ref")) || crossDomainCheck(segmentLine.getOutputs().get("file_ref")))) {
                        Boolean tester = false;
                        //It should not only be looping through segments. Ignore all cross domain and playlist files.
                        String uaIpUrl = segmentLine.getOutputs().get("user_agent") + "_" + segmentLine.getOutputs().get("ip_address") + "_" + segmentLine.getOutputs().get("path");
                        for(LogHDNLine playListLine : masterHits) {
                            String sLineUaIpUrl = playListLine.getOutputs().get("user_agent") + "_" + playListLine.getOutputs().get("ip_address") + "_" + playListLine.getOutputs().get("path");
                            //First fix the live points
                            if (uaIpUrl.equals(sLineUaIpUrl)){
                                DateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");
                                Date playListTime = timeFormat.parse(playListLine.getOutputs().get("time"));
                                Date segmentTime = timeFormat.parse(segmentLine.getOutputs().get("time"));
                                if (segmentTime.compareTo(playListTime) >= 0){
                                    playListLine.addSegment((LogHDNLine) segmentLine);
                                    tester = true;
                                    break;
                                }
                            }
                        }
                        if (!tester) {
                            //These log lines are not attributed to a playlist.
                            attributeOrphanData((LogHDNLine) segmentLine);
                            System.out.println("this segment doesn't belong to a master");
                        }
                    }
                }
            }
        } catch (Exception ex) {
            RunIt.logger.writeError(logName,Log.getLine(),ex.getMessage());
        }


        //Now that's finished. Add the cross domain lines back in to the fullHits List
        crossDomainHits.forEach((logline) -> masterHits.add(logline));
        //Now stick them in the database!
        System.out.println(tput);

        logLines.stream().forEach(x-> insertToDB(x));
      //  this.logLines.forEach(logline -> System.out.println(logline.getOutputs().get("directories")));
    }

    protected LogLine fixLive(LogLine line){

            try {
                while (liveFix.next()) {
                    try {
                        System.out.println(line.getOutputs().get("path") + "   =   " + liveFix.getString("event") + "_1@" + liveFix.getString("stream"));
                        if ((cpcode == liveFix.getInt("cpcode")) && (line.getOutputs().get("path").equals(liveFix.getString("event") + "_1@" + liveFix.getString("stream")))) {
                            System.out.println("match!");
                            line.getOutputs().put("client", liveFix.getString("client"));
                            line.getOutputs().put("directories", liveFix.getString("path"));
                            break;
                        }
                    } catch (Exception ex) {
                        RunIt.logger.writeError(logName,getLine(),ex.getMessage());
                    }
                }
                //Reset the result set
                liveFix.beforeFirst();
            } catch (SQLException ex) {
                RunIt.logger.writeError(line,ex.getMessage());
            }

        return line;

    }

    private boolean playlistCheck(String fileref){
        return ((fileref.equals("master.m3u8"))||(fileref.equals("manifest.f4m"))) ? true : false;
    }
    private boolean crossDomainCheck(String fileref){
        return fileref.equals("crossdomain.xml") ? true : false;
    }

    private void attributeOrphanData(LogHDNLine line){
        tput += Double.parseDouble(line.getOutputs().get("throughput"));
        //this.orphanLines.add(line);
    }
}
