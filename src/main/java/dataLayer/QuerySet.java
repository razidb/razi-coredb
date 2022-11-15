package dataLayer;

import org.json.simple.JSONObject;

import java.util.ArrayList;

class QuerySet {
    /* Responsible for storing the result of a query */

    ArrayList<JSONObject> documents;

    public QuerySet(ArrayList<JSONObject> documents){
        this.documents = documents;
    }

    public JSONObject first(){
        return documents.get(0);
    }

    public int count(){
        return documents.size();
    }
}
