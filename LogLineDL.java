package code;

import org.apache.commons.lang.StringEscapeUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LogLineDL extends LogLine{
    public LogLineDL(String logline, String breaker, int cpcode, ResultSet splitters){
        //Add one item to the log line
        super(logline,breaker,cpcode,splitters);
        //Now process this line
        processLine();
    }

    protected void urlSplit(){
        //First clean up the url of any unusual characters
        String urlNoHtml = StringEscapeUtils.unescapeHtml(outputs.get("full_url"));
        //Then break the url up into querystring and stem
        String[] urlArr = urlNoHtml.split("\\?");

        outputs.put("url", urlArr[0]);
        outputs.put("querystring",(urlArr.length > 1) ? urlArr[1] : "");


        //Now get the url splitters which are relevant to this cpcode
        String urlNoBase = "";
        String client = "";
        try {
            while (splitters.next()) {
                System.out.println(splitters.getString("split"));
                if(outputs.get("url").toString().indexOf(splitters.getString("split")) != -1) {
                    //Remove the base url
                    System.out.println(splitters.getString("split"));
                    urlNoBase = outputs.get("url").toString().replace(splitters.getString("split"),"");
                    client = splitters.getString("client");

                    break;
                }
            }
            //Move back to the front of the recordset
            splitters.beforeFirst();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        outputs.put("path",urlNoBase);

        //Create the directory array
        String[] dirArr = urlNoBase.split("/");

        if (client.equals("STANDARD")) {
            outputs.put("client",dirArr[0]);
        } else {
            outputs.put("client",client);
        }

        outputs.put("dir1", (dirArr.length > 2) ? dirArr[1] : "");
        outputs.put("dir2", (dirArr.length > 3) ? dirArr[2] : "");

        outputs.put("file_ref",dirArr[dirArr.length-1]);
        // outputs.put("directories", buildString(dirArr,"/"));

        String[] fileArr = outputs.get("file_ref").split("\\.");
        outputs.put("file_type", (fileArr.length > 0) ? fileArr[fileArr.length-1] : "UNKNOWN");
        //outputs.put("file_name",buildString(fileArr,"\\."));

    }
}
