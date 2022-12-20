package dataLayer;

import collection.CollectionController;
import controller.DBController;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Objects;

public class DeletionExecutor {
    private String db_name;
    private String collection_name;
    private CollectionController collectionController;

    public DeletionExecutor(String db_name, String collection_name){
        this.db_name = db_name;
        this.collection_name = collection_name;
        DBController dbController = DBController.getDbController();
        this.collectionController = dbController.getDatabase(db_name).getCollection(collection_name).getController();
    }

    public boolean execute(String _id, String indexName, String dataType){
        if (Objects.equals(dataType, "document")){
            this.collectionController.deleteDocument(_id);
        } else if (Objects.equals(dataType, "index")) {
            this.collectionController.deleteIndex(indexName);
        }
        return true;
    }
}
