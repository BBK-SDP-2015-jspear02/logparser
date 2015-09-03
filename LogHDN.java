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
                    .filter(x -> playlistCheck(x.getOutputs().get("file_ref"))) //First get all the master playlists hits by checking that they are called master.m3u8 or manifest.f4m
                         .map(line -> fixLive(line)) // Now do the live fix on them - attributing client information to them
                            .map(x -> (LogHDNLine) x) // Cast them to LogHDNLine
                                .collect(Collectors.toList());

        List<LogHDNLine> crossDomainHits = this.logLines.stream().filter(x -> (x.getOutputs().get("file_ref").equals("crossdomain.xml"))).map(x -> (LogHDNLine) x).collect(Collectors.toList());
        List<LogHDNLine> finalHits = new ArrayList<>();
        //The items are in time order. We need to reverse them and then work backwards through the list
        Collections.reverse(masterHits);

        //Reset log line count as we are reading again. This time reading the segments and seeing if they belong to a master playlist call
        lineCount = 0;

        //Now read in the segment lines one by one - making sure not to keep any references to them to avoid memory leak
        try {

            for (String line : stringLines) {
                Boolean attributed = false;
                //Create a new removal list
                List<LogHDNLine> removalList = new ArrayList<>();
                //Don't read the header lines
                if (!isHeader(line)) {
                    // Create a new temporary log line which exists for this loop
                    LogLine segmentLine = LogLineFactory.makeLogLine(this,line, breaker,cpcode,logSplitters,logType);
                    //Make sure that it is a segment rather than a playlist or a crossdomain file
                    if (!(playlistCheck(segmentLine.getOutputs().get("file_ref")) || crossDomainCheck(segmentLine.getOutputs().get("file_ref")))) {
                        Boolean tester = false;
                        //A combination of user agent, ipaddress and path should make a unique viewer
                        String uaIpUrl = segmentLine.getOutputs().get("user_agent") + "_" + segmentLine.getOutputs().get("ip_address") + "_" + segmentLine.getOutputs().get("path");
                        //Now loop through the master playlist sessions. Comparing them to this segment.
                        for(LogHDNLine playListLine : masterHits) {
                            //A combination of user agent, ipaddress and path should make a unique viewer for the master playlist
                            String mLineUaIpUrl = playListLine.getOutputs().get("user_agent") + "_" + playListLine.getOutputs().get("ip_address") + "_" + playListLine.getOutputs().get("path");

                            //Now compare the segment unique viewer to the master unique viewer - if they are the same move on to the next step.
                            //This segment might belong to this master playlist.
                            if (uaIpUrl.equals(mLineUaIpUrl)){
                                DateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");
                                //Now we compare the times of the transactions. Remember the master collection is reversed so it is going from latest to earliest
                                //so that we can f
                                Date playListTime = timeFormat.parse(playListLine.getOutputs().get("time"));
                                Date segmentTime = timeFormat.parse(segmentLine.getOutputs().get("time"));
                                //Is the playlist time less than the segment time?
                                if (segmentTime.compareTo(playListTime) <= 0){
                                   //Has it already been given to another playlist? If it has then this playlist is in the past. We can remove it.
                                    if (!attributed) {
                                        //If it hasn't already been added to a playlist
                                        playListLine.addSegment((LogHDNLine) segmentLine);
                                        attributed = true;
                                    } else {
                                        //If it has already been added to a playlist then this item can be removed
                                        //Removing items from this list can save a huge amount of time when dealing with big files
                                        removalList.add(playListLine);
                                    }
                                }
                            }
                        }
                        //We have now looped through all the master playlists. If is not an orphan segment it should be attributed.
                        //We can now safely remove any playlist items that we
                        for(LogHDNLine removalLine : removalList) {
                            //Add to the final hits and remove from the master hits.
                            finalHits.add(removalLine);
                            masterHits.remove(removalLine);
                        }
                        if (!attributed) {
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
        //re-assign back to loglines
        logLines = masterHits;
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
