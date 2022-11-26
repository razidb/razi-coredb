package collection;


import document.Document;
import org.json.simple.JSONObject;

import java.util.ArrayList;

public class Collection {
    String name;
    CollectionController controller;

    public Collection(String name, String path){
        this.name = name;
        this.controller = new CollectionController(name, path);
    }

    public String getName() {
        return name;
    }

    public CollectionController getController() {
        return controller;
    }

    public boolean createIndex(ArrayList<String> fields){
        return this.controller.createIndex(fields);
    }

    public boolean deleteIndex(String field){
        return this.controller.deleteIndex(field);
    }

    public ArrayList<Document> get(String field, String value){
        return controller.getDocument(field, value, null );
    }
}
