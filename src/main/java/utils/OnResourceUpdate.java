package utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OnResourceUpdate {
    public static void notifyReadOnlyReplicas(String resourcePath, List<String> readOnlyReplicaUrls) throws IOException {
        for(String rorUrl: readOnlyReplicaUrls){
            URL url = new URL(rorUrl + "reload-resource");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            con.setRequestMethod("GET");
            con.setRequestProperty("Content-Type", "application/json");

            Map<String, String> parameters = new HashMap<>();

            parameters.put("resourcePath", resourcePath);
            con.setDoOutput(true);

            DataOutputStream out = new DataOutputStream(con.getOutputStream());
            out.writeBytes(ParameterStringBuilder.getParamsString(parameters));
            out.flush();
            out.close();
            int status = con.getResponseCode();

            Reader streamReader = null;

            if (status > 299) {
                streamReader = new InputStreamReader(con.getErrorStream());
            } else {
                streamReader = new InputStreamReader(con.getInputStream());
            }
        }
    }
}
