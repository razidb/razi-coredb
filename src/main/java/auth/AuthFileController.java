package auth;

import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static config.Configuration.AUTH_USERS_PATH;


public class AuthFileController<K, V> {
    public static JSONArray getAllDocuments() throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        JSONObject collection = (JSONObject) parser.parse(new FileReader(AUTH_USERS_PATH));
        return (JSONArray) collection.getOrDefault("documents", new JSONArray());
    }

}
