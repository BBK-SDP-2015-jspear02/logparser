package code;


import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

/**
 * This class handles the processing of any querystring parameters with three being reserved for Groovy Gecko use
 */
public class QueryString {

    /**
     * The takes in a url and parses it using apache utils. It then loops through each key pair and if the key matches our preferred keys then it is assigned to the outputs data structure
     * @param url The url which is being processed
     * @param outputs The data structure from the log line
     * @throws URISyntaxException If there is an error in the parsing of the url
     */
    public static void processQueryString(String url,Map<String,String> outputs) throws URISyntaxException {
        List<NameValuePair> params = URLEncodedUtils.parse(new URI(url), "UTF-8");

        for (NameValuePair param : params) {
            switch (param.getName()) {
                case "ggviewid" :
                case "gghostid":
                case "ggcampaignid":
                    outputs.put(param.getName(), param.getValue());
                    break;
                default:

            }
        }
    }
}
