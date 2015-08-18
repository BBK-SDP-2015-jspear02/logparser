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
    public LogHDN(String logname,String logType, ResultSet logDetails, ResultSet logSplitters,ResultSet liveFix) throws SQLException,IOException{
        super(logname,logType,logDetails, logSplitters,liveFix);
    }
    protected void readLog() throws IOException,SQLException{
        super.readLog();
        analyseLog();
    }

    protected void analyseLog() throws SQLException{
        //Now all of the loglines have been created we need to strip out the partial requests to process them as individual lines

        List<LogHDNLine> masterHits = this.logLines.stream().filter(x -> playlistCheck(x.getOutputs().get("file_ref"))).map(x -> (LogHDNLine) x).collect(Collectors.toList());
        List<LogHDNLine> segmentHits = this.logLines.stream().filter(x -> !(playlistCheck(x.getOutputs().get("file_ref")))).map(x -> (LogHDNLine) x).collect(Collectors.toList());
        List<LogHDNLine> crossDomainHits = this.logLines.stream().filter(x -> (x.getOutputs().get("file_ref").equals("crossdomain.xml"))).map(x -> (LogHDNLine) x).collect(Collectors.toList());

        //The items are in time order. We need to reverse them.
        Collections.reverse(masterHits);
        Collections.reverse(segmentHits);

        for(LogHDNLine line : masterHits) {
            //Now loop through them. If they are already set then add to the original. Otherwise put it in there.
            String uaIpUrl = line.getOutputs().get("user_agent") + "_" + line.getOutputs().get("ip_address") + "_" + line.getOutputs().get("path");
            for(LogHDNLine sLine : segmentHits) {
                String sLineUaIpUrl = sLine.getOutputs().get("user_agent") + "_" + sLine.getOutputs().get("ip_address") + "_" + sLine.getOutputs().get("path");
                //First fix the live points
                while (liveFix.next()) {
                    try {
                        System.out.println();
                        if ((cpcode == liveFix.getInt("cpcode")) && (line.getOutputs().get("path").equals(liveFix.getString("event") + "_1@" + liveFix.getString("stream")))) {
                            line.getOutputs().put("client", liveFix.getString("client"));
                            line.getOutputs().put("directories", liveFix.getString("path"));
                            break;
                        }
                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }
                //Reset the result set
                liveFix.beforeFirst();

                if (uaIpUrl.equals(sLineUaIpUrl)){
                    DateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");
                    try {
                        Date playListTime = timeFormat.parse(line.getOutputs().get("time"));
                        Date segmentTime = timeFormat.parse(sLine.getOutputs().get("time"));
                        if (segmentTime.compareTo(playListTime) >= 0){
                            line.addSegment(sLine);
                        } else {
                            //Leave the for loop.
                            break;
                        }
                    } catch (ParseException ex){
                        System.out.println("couldn't parse time!");
                    } catch (Exception ex){
                        System.out.println("something wrong");
                    }

                }
            }
        }
        //Now fix the live points

        //Now that's finished. Add the lines back in to the fullHits List
        crossDomainHits.forEach((logline) -> masterHits.add(logline));
        //Now stick them in the database!
        masterHits.stream().forEach(x-> insertToDB(x));

    }

    private boolean playlistCheck(String fileref){
        return ((fileref.equals("master.m3u8"))||(fileref.equals("manifest.f4m"))) ? true : false;
    }
}
