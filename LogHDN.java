package code;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
/**
 * This is the HDN Log format, it is a child of the Log class and handles the processing of any HDS/HLS log files.
 */
public class LogHDN extends Log{
    private double tput;
    public LogHDN(String logname, ResultSet logDetails, ResultSet logSplitters,ResultSet liveFix,Database db, LogReader reader, ErrorLog logger, Boolean debug) throws Exception{
        super(logname,logDetails, logSplitters,liveFix,db, reader, logger, debug);
        tput = 0;
    }

    /**
     * This reads in the log. HDN logs require a special reading in method because they contain so many rows they can cause the system to run out of memory.
     * It keeps objects, only for the playlist requests and crossdomain requests which are used in the analysis.
     * @throws Exception Handles any exceptions which are passed up the chain when possible.
     */
    @Override
    protected void readLog() throws Exception{
        System.out.println("START: Read log....");
        lineCount = 0;
        errorCount = 0;
        stringLines = reader.OpenReader(logName);
        //Convert the strings into objects of type logline after checking that they aren't header lines

            this.logLines = stringLines.stream().
                    filter(x -> (!(isHeader(x)))) // check it isn't a header line
                    .map(y -> LogLineFactory.makeLogLine(this, y, breaker, logSplitters, logType, logger)) // create the object
                    .filter(line -> (playlistCheck(line.getOutputs().get("file_ref")) || crossDomainCheck(line.getOutputs().get("file_ref")))) //Check it is a playlist or crossdomain request
                    .collect(Collectors.toList());

        System.out.println("COMPLETE: READ log....");
        //Now analyse it!
        analyzeLog();
    }

    /**
     * This analyses the log file, attributing segment hits to playlist requests.
     * @throws Exception Handles any exceptions which are passed up the chain when possible.
     */
    protected void analyzeLog() throws Exception{
        //All of the playlist lines have been created. Now we need to go through the segments again, adding them to the right playlist.
        System.out.println("START: Analyze log. This can take several minutes.");
        List<LogHDNLine> masterHits = this.logLines.stream()
                    .filter(x -> playlistCheck(x.getOutputs().get("file_ref"))) //First get all the master playlists hits by checking that they are called master.m3u8 or manifest.f4m
                            .map(x -> (LogHDNLine) x) // Cast them to LogHDNLine
                                .collect(Collectors.toList());

        //Now get all the cross domain hits and seperate them into their own list. They will be added back in later
        List<LogHDNLine> crossDomainHits = this.logLines.stream().filter(x -> (x.getOutputs().get("file_ref").equals("crossdomain.xml"))).map(x -> (LogHDNLine) x).collect(Collectors.toList());
        //This is the final list that will be entered into the database - currently empty
        List<LogHDNLine> finalHits = new ArrayList<>();
        //The items are in time order. We need to reverse them and then work backwards through the list
        Collections.reverse(masterHits);
        //Reset log line count as we are reading again. This time reading the segments and seeing if they belong to a master playlist call
        lineCount = 0;
        //Now read in the segment lines one by one - making sure not to keep any references to them to avoid memory leak
        try {

            for (String line : stringLines) {
                Boolean attributed = false; //Specifies whether the segment has already been attributed
                //Create a new removal list
                List<LogHDNLine> removalList = new ArrayList<>(); // This is used to remove items from the master list when they are in the past to allow for quicker scanning
                //Don't read the header lines
                if (!isHeader(line)) {
                    // Create a new temporary log line which exists for this loop
                    LogLine segmentLine = LogLineFactory.makeLogLine(this,line, breaker,logSplitters,logType, logger);
                    //Make sure that it is a segment rather than a playlist or a crossdomain file
                    if (!(playlistCheck(segmentLine.getOutputs().get("file_ref")) || crossDomainCheck(segmentLine.getOutputs().get("file_ref")))) {
                        //A combination of user agent, ipaddress and path should make a unique viewer for this segment
                        String uaIpUrl = segmentLine.getOutputs().get("user_agent") + "_" + segmentLine.getOutputs().get("ip_address") + "_" + segmentLine.getOutputs().get("stream");
                        //Now loop through the master playlist sessions. Comparing them to this segment.
                        for(LogHDNLine playListLine : masterHits) {
                            //A combination of user agent, ipaddress and path should make a unique viewer for the master playlist
                            String mLineUaIpUrl = playListLine.getOutputs().get("user_agent") + "_" + playListLine.getOutputs().get("ip_address") + "_" + playListLine.getOutputs().get("stream");

                            //Now compare the segment unique viewer to the master unique viewer - if they are the same move on to the next step.
                            //This segment might belong to this master playlist.
                            if (uaIpUrl.equals(mLineUaIpUrl)){
                                DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                //Now we compare the times of the transactions. Remember the master collection is reversed so it is going from latest to earliest
                                //so that we can find the last playlist called by this user.
                                Date playListTime = timeFormat.parse(playListLine.getOutputs().get("date") + " " + playListLine.getOutputs().get("time"));
                                Date segmentTime = timeFormat.parse(segmentLine.getOutputs().get("date") + " " + segmentLine.getOutputs().get("time"));
                                //Is the playlist time less than the segment time?
                                if (segmentTime.compareTo(playListTime) >= 0){
                                   //Has it already been given to another playlist? If it has then this playlist is in the past and won't be needed again. We can remove it.
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
                        //We have now looped through all the master playlists. The segment should be attributed.
                        //We can now safely remove any playlist items that we have placed in the removal queue which will speed up the next loop.
                        for(LogHDNLine removalLine : removalList) {
                            //Add to the final hits and remove from the master hits.
                            finalHits.add(removalLine);
                            masterHits.remove(removalLine);
                        }
                    }
                }
            }

        } catch (Exception ex) {
            throw new Exception("Error when analysing HDN log on line " + Log.getLine());
        }

        //Now add any items that were never removed from the master hits
        finalHits.addAll(masterHits);
        //Now add the crossdomain hits back in
        finalHits.addAll(crossDomainHits);
        //Now loop through all items in master hits and attribute live data to them
        for (LogLine hit : finalHits) {
            hit = fixLive(hit);
        }
        System.out.println("COMPLETE: Analyze log.");
        //re-assign back to loglines
        logLines = finalHits;
        //Insert to the tempporary database table
        insertAllLinesToTempDB();

    }

    /**
     * This handles attributing data to a client and directory. It matches up lines with the fix_live table and then re-processes part of the line so that it has the correct client/ directory information.
     * @param line The log line that is currently being checked to see if it can be attributed to a client
     * @return The mutated line object with client data attributed
     * @throws SQLException Handles any exceptions which are passed up the chain when possible.
     * */
    protected LogLine fixLive(LogLine line) throws SQLException{

                while (liveFix.next()) {
                       // System.out.println(line.getOutputs().get("stream") + "   =   " + liveFix.getString("event") + "_1@" + liveFix.getString("stream"));
                        if ((cpcode == liveFix.getInt("cpcode")) && (line.getOutputs().get("stream").equals(liveFix.getString("event") + "_1@" + liveFix.getString("stream")))) {

                            line.getOutputs().put("client", liveFix.getString("client"));
                            line.getOutputs().put("path", liveFix.getString("path") + "/" + line.getOutputs().get("file_ref"));
                            //Now a path has been set, analyse this url.
                            Url.directorySplit(line.getOutputs().get("path"), line.getOutputs());
                            break;
                        }
                }
                //Reset the result set
                liveFix.beforeFirst();

        return line;

    }

    /**
     * This checks whether the item is a master playlist or not.
     * @param fileref the name of the file
     * */
    private boolean playlistCheck(String fileref){
        return ((fileref.equals("master.m3u8"))||(fileref.equals("manifest.f4m"))) ? true : false;
    }

    /**
     * This checks whether the item is a crossdomain file or not.
     * @param fileref the name of the file
     * */
    private boolean crossDomainCheck(String fileref){
        return fileref.equals("crossdomain.xml") ? true : false;
    }

}
