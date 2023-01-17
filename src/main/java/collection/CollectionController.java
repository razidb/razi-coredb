package collection;

import bst.Node;
import document.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import queryParserLayer.clauses.Operations;

import java.io.IOException;
import java.util.*;

public class CollectionController {
    String name;
    String path;
    Vector<Index> indexes;
    Index<String, Document> mainIndex;
    CollectionFileController fileController;

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
            String indexName = String.valueOf(indexFromFile.getOrDefault("name", ""));
            if (Objects.equals(indexName, "default")){
                // create default index
                ArrayList<String> mainIndexFields = new ArrayList<>();
                mainIndexFields.add("_id");
                this.mainIndex = new Index<>(mainIndexFields);
            }
            else{
                ArrayList<String> fields = (ArrayList<String>) indexFromFile.getOrDefault("fields", new ArrayList<>());
                if (fields == null)
                    continue;
                Index<String, Document> index = new Index<>(fields);
                this.indexes.add(index);
            }
        }
    }

    private void loadDataToIndexes() throws IOException, ParseException{
        /*
        * The actual indexing of data into binary search trees
        * */
        JSONParser jsonParser = new JSONParser();
        List<String> documentsFromFile = fileController.getAllDocumentsNew();

        for (String o : documentsFromFile) {
            JSONObject documentObject = (JSONObject) jsonParser.parse(o);
            Document document = new Document(documentObject.get("_id").toString(), o);

            // insert the document in the main index
            this.mainIndex.bst.Insert(document.getId(), document);
            // iterate over user-defined indexes
            for (Index index : indexes) {
                // single field index
                if (index.fields.size() == 1){
                    String indexedField = String.valueOf(index.fields.get(0));
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
                        String value = String.valueOf(documentObject.getOrDefault(field, ""));
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

    private void loadDataToIndex(Index<String, Document> index) throws IOException, ParseException {
        /*
        * Load data to a specific index.
        * This can be used when a specific index is created, and we need
        * to load collection data to that specific index only.
        * */
        JSONParser parser = new JSONParser();
        List<String> documentsFromFile = fileController.getAllDocumentsNew();
        for (Object o : documentsFromFile) {
            JSONObject documentObject = (JSONObject) parser.parse((String) o);
            Document document = new Document(documentObject.get("_id").toString(), documentObject.toJSONString());
            if (index.fields.size() == 1){
                String indexedField = index.fields.get(0);
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
                    String value = String.valueOf(documentObject.getOrDefault(field, ""));
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

    public boolean deleteIndex(ArrayList<String> fields){
        Index index = findIndex(fields);

        if (index == null){
            System.out.println("Index with fields: " + fields + " does not exist");
            return false;
        }
        // remove index from file
        try{
            this.fileController.deleteIndex(fields);
        }
        catch (Exception e){
            System.out.println("Something went wrong while deleting index " + index);
            System.out.println(e.getMessage());
            return false;
        }
        // remove index from memory
        this.indexes.remove(index);
        return true;
    }

    public Index<String, Document> findIndex(ArrayList field_set){
        Index index = null;
        boolean single_field_lookup = field_set.size() == 1;

        if (single_field_lookup && field_set.contains("_id")){
            index = mainIndex;
        }
        else{
            for (Index i: indexes){
                int index_fields_size = i.fields.size();
                if (index_fields_size == 1 && single_field_lookup){
                    if (Objects.equals(i.fields.get(0), field_set.get(0))){
                        index = i;
                        break;
                    }
                }
                else if (index_fields_size > 1 && !single_field_lookup){
                    if (index_fields_size == ((ArrayList<?>) field_set).size() && i.fields.containsAll(((ArrayList<?>) field_set))){
                        index = i;
                        break;
                    }
                }
            }
        }
        return index;
    }

    public ArrayList<Document> getDocument(ArrayList field_set, Object value_set, Index<String, Document> index, Operations operation){
        // single field
        if (index == null)
            index = findIndex(field_set);

        if (index != null){
            Node<String, Document> node = index.bst.search(String.valueOf(value_set));
            ArrayList<Document> result = new ArrayList<>();
            if (node != null){
                result.addAll(node.values);
            }
            return result;
        }
        else{
            // scan the file for the data provided
            return null;
        }
    }

    public void addDocumentsToCollection(JSONArray documents){
        try{
            CollectionFileController.writeDocumentsToCollection(path, documents);
        }
        catch (Exception exception){

        }
    }

    public void addDocument(JSONObject documentObject){
        try{
            // add document to file
            // --- OLD IMPLEMENTATION ---
            // documentObject.put("_id", UUID.randomUUID().toString());
            // CollectionFileController.addDocument(path, documentObject);

            // --- NEW IMPLEMENTATION ---
            CollectionFileController.insertDocument(path, documentObject);

            // add document to indexes
            Document document = new Document(documentObject.get("_id").toString(), documentObject.toJSONString());
            // insert the document in the main index
            this.mainIndex.bst.Insert(document.getId(), document);
            // iterate over user-defined indexes
            for (Index index : indexes) {
                // single field index
                if (index.fields.size() == 1){
                    String indexedField = String.valueOf(index.fields.get(0));
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
                        String value = String.valueOf(documentObject.getOrDefault(field, ""));
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
        catch (Exception exception){

        }
    }


    public boolean deleteDocument(String _id){
        try{
            Node<String, Document> documentNode = this.mainIndex.getBst().search(_id);
            JSONObject jsonDocument = documentNode.value.getValueAsJSON();

            // delete from file
            boolean deleted = CollectionFileController.deleteDocument(path, Integer.parseInt(_id));
            System.out.println("Document with ID is deleted from disk: " + deleted);
            // delete document from memory (indexes)
            // delete from main index
            this.mainIndex.bst.delete(_id, null);
            for (Index index: indexes){
                if (index.fields.size() == 1){
                    String indexedField = String.valueOf(index.fields.get(0));
                    if (jsonDocument.containsKey(indexedField)) {
                        String indexedValue = jsonDocument.getOrDefault(indexedField, "").toString();
                        index.bst.delete(indexedValue, _id);
                    }
                }
                else if (index.fields.size() > 1){
                    // multi-field index
                    // index value will be represented as: val1-val2-val3-...
                    ArrayList<String> values = new ArrayList<>();
                    for (Object field: index.fields){
                        String value = String.valueOf(jsonDocument.getOrDefault(field, ""));
                        values.add(value);
                    }
                    String indexedValue = String.join(
                            MULTI_FIELD_INDEX_VALUES_SEPARATOR,
                            values
                    );
                    index.bst.delete(indexedValue, _id);
                }
            }
            return true;
        } catch (Exception e){
            System.out.println(e.getMessage());
            return false;
        }
    }

    public boolean updateDocument(String _id, Map<String, Object> data) throws Exception {
        try{
            Node<String, Document> documentNode = this.mainIndex.getBst().search(_id);
            Document document = documentNode.value;
            JSONObject jsonDocument = document.getValueAsJSON();

            JSONObject newJsonDocument = new JSONObject(data);
            document.setData(newJsonDocument.toString());

            // delete from file
            boolean deleted = CollectionFileController.updateDocument(path, Integer.parseInt(_id), data);
            System.out.println("Document with ID is updated from disk: " + deleted);

            // delete document from memory (indexes)
            for (Index index: indexes){
                String indexedValue = null;
                if (index.fields.size() == 1){
                    String indexedField = String.valueOf(index.fields.get(0));
                    if (jsonDocument.containsKey(indexedField)) {
                        indexedValue = jsonDocument.getOrDefault(indexedField, "").toString();
                    }
                }
                else if (index.fields.size() > 1){
                    // multi-field index
                    // index value will be represented as: val1-val2-val3-...
                    ArrayList<String> values = new ArrayList<>();
                    for (Object field: index.fields){
                        String value = String.valueOf(jsonDocument.getOrDefault(field, ""));
                        values.add(value);
                    }
                    indexedValue = String.join(
                        MULTI_FIELD_INDEX_VALUES_SEPARATOR, values
                    );

                }
                if (indexedValue != null){
                    index.bst.delete(indexedValue, _id);
                    index.bst.Insert(indexedValue, document);
                    System.out.println("Document is updated on index ");
                }
            }
            return true;
        } catch (Exception e){
            throw new Exception(e);
        }
    }

}
