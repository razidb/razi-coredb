package dataLayer;

import org.json.simple.JSONArray;
import controller.DBController;
import collection.CollectionController;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Objects;

public class InsertionExecutor {
    private String db_name;
    private String collection_name;
    private CollectionController collectionController;

    public InsertionExecutor(String db_name, String collection_name){
        this.db_name = db_name;
        this.collection_name = collection_name;
        DBController dbController = DBController.getDbController();
        this.collectionController = dbController.getDatabase(db_name).getCollection(collection_name).getController();
    }

    public boolean execute(Object data, String dataType){
        if (Objects.equals(dataType, "document")){
            if (data instanceof JSONObject)
                this.collectionController.addDocument((JSONObject) data);
            else if (data instanceof JSONArray) {
                // TODO: implement
            }
        } else if (Objects.equals(dataType, "index")) {
            this.collectionController.createIndex((ArrayList<String>) data);
        }
        return true;
    }
}
