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
    Vector<Index> indexes;

    private final String MULTI_FIELD_INDEX_VALUES_SEPARATOR = "-";

    public CollectionController(String name, String path){
        this.name = name;
        this.path = path;
        this.fileController = new CollectionFileController(path);
        this.indexes = new Vector<>();
        try{
            this.loadIndexes();
            this.loadDataToIndexes();
        }
        catch (Exception ignored){
            throw new RuntimeException(ignored);
        }

    }

    public String getPath() {
        return path;
    }

    private void loadIndexes() throws IOException, ParseException {
        // get indexed fields from collection file
        ArrayList<JSONObject> indexesFromFile = fileController.getIndexes();
        for (JSONObject indexFromFile : indexesFromFile) {
            ArrayList<String> fields = (ArrayList<String>) indexFromFile.getOrDefault("fields", new ArrayList<>());
            if (fields == null)
                continue;
            Index<String, Document> index = new Index<>(fields);
            this.indexes.add(index);
        }
    }

    private void loadDataToIndexes() throws IOException, ParseException{
        /*
        * The actual indexing of data into binary search trees
        * */
        JSONArray documentsFromFile = fileController.getAllDocuments();
        for (Object o : documentsFromFile) {
            JSONObject documentObject = (JSONObject) o;
            Document document = new Document(documentObject.get("_id").toString(), documentObject.toJSONString());
            for (Index index : indexes) {
                // single field index
                if (index.fields.size() == 1){
                    String indexedField = (String) index.fields.get(0);
                    if (documentObject.containsKey(indexedField)) {
                        String indexedValue = documentObject.getOrDefault(indexedField, "").toString();
                        index.bst.Insert(indexedValue, document);
                    }
                }
                else if (index.fields.size() > 1){
                    // multi-field index
                    // index value will be represented as: val1-val2-val3-...
                    ArrayList<String> values = new ArrayList<>();
                    for (Object field: index.fields){
                        String value = (String) documentObject.getOrDefault(field, "");
                        values.add(value);
                    }
                    String indexedValue = String.join(
                        MULTI_FIELD_INDEX_VALUES_SEPARATOR,
                        values
                    );
                    index.bst.Insert(indexedValue, document);
                }
                // index has no fields
                else{
                    // do nothing
                }
            }
        }
    }

    private void loadDataToIndex(Index index) throws IOException, ParseException {
        /*
        * Load data to a specific index.
        * This can be used when a specific index is created, and we need
        * to load collection data to that specific index only.
        * */
        JSONArray documentsFromFile = fileController.getAllDocuments();
        for (Object o : documentsFromFile) {
            JSONObject documentObject = (JSONObject) o;
            Document document = new Document(documentObject.get("_id").toString(), documentObject.toJSONString());
            if (index.fields.size() == 1){
                String indexedField = (String) index.fields.get(0);
                if (documentObject.containsKey(indexedField)) {
                    String indexedValue = documentObject.getOrDefault(indexedField, "").toString();
                    index.bst.Insert(indexedValue, document);
                }
            }
            else if (index.fields.size() > 1){
                // multi-field index
                // index value will be represented as: val1-val2-val3-...
                ArrayList<String> values = new ArrayList<>();
                for (Object field: index.fields){
                    String value = (String) documentObject.getOrDefault(field, "");
                    values.add(value);
                }
                String indexedValue = String.join(
                        MULTI_FIELD_INDEX_VALUES_SEPARATOR,
                        values
                );
                index.bst.Insert(indexedValue, document);
            }
            // index has no fields
            else{
                // do nothing
            }
        }
    }

    public boolean createIndex(ArrayList<String> fields) {
        /* create a new index, then load data to it */

        ArrayList<JSONObject> indexes;
        try {
            indexes = fileController.getIndexes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // make sure requested index does not already exist
        for (JSONObject index: indexes){
            // TODO: make sure index does not already exist
        }

        // try to add the new index in the collection json file
        try{
            fileController.createIndex(fields);
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }

        Index<String, Document> index = new Index<String, Document>(fields);
        this.indexes.add(index);

        try{
            loadDataToIndex(index);
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }

        return true;
    }
    public boolean deleteIndex(String field) {
        ArrayList<JSONObject> indexes;
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

        this.indexes.removeElement(field);
        return true;
    }

    public ArrayList<JSONObject> getDocument(String field, String value){
        // try single-indexed fields
        Index indexedField = null;
        for (Index index: indexes){
            if (index.fields.size() == 1 && Objects.equals(index.fields.get(0), field)){
                indexedField = index;
                break;
            }
            else if (index.fields.size() > 1){
                String fields[] = field.split(MULTI_FIELD_INDEX_VALUES_SEPARATOR);
                for (String i: fields){
                    if (index.fields.contains(i)){
                        indexedField = index;
                        break;
                    }
                }
            }
        }
        // if the field is indexed
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
