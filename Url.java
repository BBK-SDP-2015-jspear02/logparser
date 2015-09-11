package code;


import org.apache.commons.lang.StringEscapeUtils;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class handles the breaking down of urls into more meaningful data so that is can be assigned to clients.
 */
public class Url {
    /**
     * This handles the initial breakdown of the url, seperating url and querystring and using the splitters recordset to break up the url
     * @param outputs  The map data structure which is used for each log line
     * @param splitters The recordset which contains each splitter string
     * @return A string array featuring the url once the splitter has been removed and the type of client
     * @throws SQLException If there is an error when looping through the recordset
     * @throws URISyntaxException If there is an error when processing the querystring
     */
    public static String[] urlSplitBasic(Map<String,String> outputs,ResultSet splitters) throws SQLException, URISyntaxException{

        //First clean up the url of any unusual characters
        String urlNoHtml = StringEscapeUtils.unescapeHtml(outputs.get("full_url"));
        //Then break the url up into querystring and stem
        String[] urlArr = urlNoHtml.split("\\?");

        outputs.put("url", urlArr[0]);
        outputs.put("querystring",(urlArr.length > 1) ? urlArr[1] : "");

        //Break down the querystring - but only if it exists.
        if (!(outputs.get("querystring").equals(""))) {
            QueryString.processQueryString(urlNoHtml,outputs);
        }

        //Now get the url splitters which are relevant to this cpcode
        String[] rtnArray = new String[2];

        try {
            while (splitters.next()) {
                if(outputs.get("url").toString().indexOf(splitters.getString("split")) != -1) {
                    //Remove the base url
                    rtnArray[0] = outputs.get("url").toString().replace(splitters.getString("split"),"");
                    rtnArray[1] = splitters.getString("client");
                    break;
                }
            }
            //Move back to the front of the recordset
            splitters.beforeFirst();
        } catch (SQLException ex) {
            throw new SQLException("Error while trying to assign splitter on URL.");
        }
        return rtnArray;
    }

    /**
     * First does the basic url split and then assigns data by doing the directory split with the returned data from basic split
     * @param outputs  The map data structure which is used for each log line
     * @param splitters The recordset which contains each splitter string
     * @throws SQLException If there is an error when looping through the recordset
     * @throws URISyntaxException If there is an error when processing the querystring
     */
    public static void urlSplit(Map<String,String> outputs,ResultSet splitters) throws SQLException, URISyntaxException{
        String[] basicInfo = urlSplitBasic(outputs,splitters);
        outputs.put("client",basicInfo[1]);
        //Now process the rest of the url
        directorySplit(basicInfo[0],outputs);
    }

    /**
     * You now have a path of format dir1/dir2/dir3/filename.file extention so this needs to be broken up into individual directories
     * @param path The path that needs to be broken up
     * @param outputs The map data structure which is used for each log line
     */
    public static void directorySplit(String path,Map<String,String> outputs) {
        //Create the directory array
        List<String> dirList = new LinkedList<String>(Arrays.asList(path.split("/")));
        if (outputs.get("client").equals("STANDARD")) {
            outputs.put("client", dirList.remove(0));
        }

        outputs.put("dir1", (dirList.size() > 1) ? dirList.get(0) : "");
        outputs.put("dir2", (dirList.size() > 2) ? dirList.get(1) : "");

        outputs.put("file_ref",(dirList.size() > 0) ? dirList.get(dirList.size()-1) : "");
        outputs.put("directories", buildString(dirList.toArray(new String[dirList.size()]),"/"));
        outputs.put("path",outputs.get("directories") + "/" + outputs.get("file_ref"));
        //Now split at file level
        fileSplit(outputs);
    }

    /**
     * Any live or HDN logs will need to be processed using this. Rather than data coming through in a normal way, data is read in by stream which then needs
     * to be matched with an item from the fix_live recordset which will assign it to a client.
     * @param outputs The map data structure which is used for each log line
     * @param splitters The recordset which contains each splitter string
     * @throws SQLException If there is an error when looping through the recordset
     * @throws URISyntaxException If there is an error when processing the querystring
     */
    public static void urlSplitLive(Map<String,String> outputs,ResultSet splitters) throws SQLException, URISyntaxException{
        String[] basicInfo = urlSplitBasic(outputs, splitters);

        //Create the directory array
        String[] dirArr = basicInfo[0].split("/");
        outputs.put("segment_count", "0");
        outputs.put("duration", "0");

        //HDN follows a special pattern. If the first directory is I then it is for HLS otherwise it is HDS
        if (dirArr.length > 1) {
            outputs.put("format", (dirArr[0].equals("i")) ? "HLS" : "HDS");
            outputs.put("stream", dirArr[1]);
            outputs.put("file_ref", dirArr[2]);
            if((outputs.get("file_ref").indexOf(".m3u8")== -1 ) && (outputs.get("file_ref").indexOf("f4m")== -1 )) {
                //Not a playlist file
                String[] itemSplit = outputs.get("file_ref").split("_");
                //Get the bandwidth being watched in kbps
                outputs.put("bandwidth", (outputs.get("format").equals("HDS")) ? itemSplit[0] : itemSplit[1] );
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
        //Now split at a file level
        fileSplit(outputs);
    }


    /**
     * Break up the filename string into file_ref (eg. master.m3u8), file_type (eg. m3u8) and file_name (eg. master)
     * @param outputs The map data structure which is used for each log line
     */
    private static void fileSplit(Map<String,String> outputs) {
        String[] fileArr = outputs.get("file_ref").split("\\.");
        outputs.put("file_type", (fileArr.length > 0) ? fileArr[fileArr.length-1] : "UNKNOWN");
        outputs.put("file_name", buildString(fileArr,"\\."));
    }

    /**
     * A method for rebuilding a string after it has been broken apart into an array - similar to implode in php
     * @param strArr The array that is being converted into a string
     * @param join The string that is used to join them
     * @return The joined string
     */
    private static String buildString(String[] strArr, String join) {
        String joinedOutput = "";
        for (int i = 0; i < strArr.length; i++) {
            joinedOutput += (i == (strArr.length - 1)) ? "" : strArr[i] + ((i == strArr.length - 2) ? "" : join);
        }
        return joinedOutput;
    }


}
