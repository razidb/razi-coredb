package document;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Document {
    String _id;
    String data;

    public Document(String _id, String data){
        this._id = _id;
        this.data = data;
    }

    public String getId() {
        return _id;
    }

    public JSONObject getValueAsJSON(){
        JSONParser parser = new JSONParser();
        try{
            return (JSONObject) parser.parse(data);
        }
        catch (Exception e){
            return null;
        }
    }

    @Override
    public String toString() {
        return this.data.toString();
    }
}
