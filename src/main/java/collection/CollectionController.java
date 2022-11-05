package collection;

import bst.Node;
import document.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Vector;

public class CollectionController {
    String name;
    String path;
    CollectionFileController fileController;
    Vector<Index> indexedFields;

    public CollectionController(String name, String path){
        this.name = name;
        this.path = path;
        this.fileController = new CollectionFileController(path);
        this.indexedFields = new Vector<>();
        try{
            this.loadIndexes();
            this.loadDataToIndexes();
        }
        catch (Exception ignored){
            throw new RuntimeException(ignored);
        }

    }

    private void loadIndexes() throws IOException, ParseException {
        // get indexed fields from collection file
        ArrayList<String> indexesFromFile = fileController.getIndexes();
        for (String indexFromFile : indexesFromFile) {
            Index<String, Document> index = new Index<String, Document>(indexFromFile);
            this.indexedFields.add(index);
        }
    }

    private void loadDataToIndexes() throws IOException, ParseException{
        JSONArray documentsFromFile = fileController.getAllDocuments();
        for (Object o : documentsFromFile) {
            JSONObject documentObject = (JSONObject) o;
            Document document = new Document(documentObject.get("_id").toString(), documentObject.toJSONString());
            for (Index index : indexedFields) {
                if (documentObject.containsKey(index.fieldName)) {
                    String indexedValue = documentObject.getOrDefault(index.fieldName, "").toString();
                    index.bst.Insert(indexedValue, document);
                }
            }
        }
    }

    private void loadDataToIndex(Index index) throws IOException, ParseException {
        JSONArray documentsFromFile = fileController.getAllDocuments();
        for (Object o : documentsFromFile) {
            JSONObject documentObject = (JSONObject) o;
            Document document = new Document(documentObject.get("_id").toString(), documentObject.toJSONString());
            if (documentObject.containsKey(index.fieldName)) {
                String indexedValue = documentObject.getOrDefault(index.fieldName, "").toString();
                index.bst.Insert(indexedValue, document);
            }
        }
    }

    public boolean createIndex(String field) {
        ArrayList<String> indexes;
        try {
            indexes = fileController.getIndexes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (indexes.contains(field)){
            return false;
        }

        try{
            fileController.createIndex(field);
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }

        Index<String, Document> index = new Index<String, Document>(field);
        this.indexedFields.add(index);

        try{
            loadDataToIndex(index);
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }

        return true;
    }
    public boolean deleteIndex(String field) {
        ArrayList<String> indexes;
        try {
            indexes = fileController.getIndexes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!indexes.contains(field)){
            return false;
        }

        try{
            fileController.deleteIndex(field);
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }

        this.indexedFields.removeElement(field);
        return true;
    }

    public ArrayList<JSONObject> getDocument(String field, String value){
        // try indexed fields
        Index indexedField = null;
        for (Index index: indexedFields){
            if (Objects.equals(index.fieldName, field)){
                indexedField = index;
                break;
            }
        }

        if (indexedField != null){
            Node object = indexedField.bst.search(value);
            ArrayList<JSONObject> result = new ArrayList<>();
            if (object!= null){
                for (Object d: object.values){
                    Document dd = (Document) d;
                    result.add(dd.getValueAsJSON());
                }
            }
            return result;
        }
        else{
            // scan the file for the data provided
            return null;
        }
    }
}
