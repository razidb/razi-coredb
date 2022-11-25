package dataLayer;

import document.Document;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;

class QuerySet {
    /* Responsible for storing the result of a query */

    ArrayList<Document> documents;

    public QuerySet(ArrayList<Document> documents){
        this.documents = documents;
    }

    public Document first(){
        return documents.get(0);
    }

    public int count(){
        return documents.size();
    }

    void intersect(QuerySet querySet){
        /* do intersection operation between two query sets */

        if (this.documents == null){
            this.documents = querySet.documents;
            return;
        } else if (querySet.documents == null) {
            return;
        }

        HashSet<String> ids_set = new HashSet<>();
        ArrayList<Document> result = new ArrayList<>();
        // iterate over documents to be intersected
        for (Document document: querySet.documents){
            ids_set.add(document.getId());
        }

        for (Document document: this.documents){
            boolean document_exists = ids_set.contains(document.getId());
            if (document_exists){
                result.add(document);
            }
        }

        // assign result to current queryset
        this.documents = result;
    }

    void merge(QuerySet querySet){

        if (this.documents == null){
            this.documents = querySet.documents;
            return;
        } else if (querySet.documents == null) {
            return;
        }

        HashSet<String> ids_set = new HashSet<>();
        ArrayList<Document> result = new ArrayList<>();

        for (Document document: this.documents){
            ids_set.add(document.getId());
            result.add(document);
        }

        for (Document document: querySet.documents){
            String document_id = document.getId();
            boolean document_exist = ids_set.contains(document_id);
            if (!document_exist){
                ids_set.add(document_id);
                result.add(document);
            }
        }

        // assign result to current queryset
        this.documents = result;
    }
}
