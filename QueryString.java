package code;


import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class QueryString {


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
