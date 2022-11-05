package auth;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;

import static config.Configuration.AUTH_USERS_PATH;


public class AuthFileController {
    static String authUsersPath = AUTH_USERS_PATH;

    public static JSONArray getAllDocuments() throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        JSONObject collection = (JSONObject) parser.parse(new FileReader(authUsersPath));
        JSONArray documentsFromFile = (JSONArray) collection.getOrDefault("documents", new JSONArray());
        return documentsFromFile;
    }

}
