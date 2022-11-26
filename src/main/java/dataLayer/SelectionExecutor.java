package dataLayer;

import collection.Collection;
import collection.CollectionController;
import collection.CollectionFileController;
import collection.Index;
import controller.DBController;
import document.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import queryParserLayer.clauses.BinaryOperation;
import queryParserLayer.clauses.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.StreamSupport;

public class SelectionExecutor {
    /* Responsible for executing optimized selection over a set of operations */
    private String db_name;
    private String collection_name;
    private CollectionController collectionController;

    public SelectionExecutor(String db_name, String collection_name){
        this.db_name = db_name;
        this.collection_name = collection_name;
        DBController dbController = DBController.getDbController();
        this.collectionController = dbController.getDatabase(db_name).getCollection(collection_name).getController();
    }

    public ArrayList<Document> execute(ArrayList<Operation> operations, boolean match_all){
        ArrayList<Document> result;
        // construct fields

        ArrayList<String> fields = new ArrayList<>();
        ArrayList<String> values = new ArrayList<>();
        for (Operation operation: operations){
            switch (operation.type) {
                case EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    BinaryOperation binary_operation = (BinaryOperation) operation;
                    fields.add(binary_operation.operand1);
                    values.add(binary_operation.operand2);
                }
                case IN -> {}
                case LIKE -> {}
                case IS_NULL -> {}
            }
        }
        // try to find an index for the passed field/s
        Index<String, Document> index = collectionController.findIndex(fields);
        if (index != null){
            ArrayList<String> value = new ArrayList<>();
            for (String v: index.getFields()){
                int pos_of_v = fields.indexOf(v);
                String corresponding_value = values.get(pos_of_v);
                value.add(corresponding_value);
            }
            String value_str = String.join("-", value);
            result = collectionController.getDocument(fields, value_str, index);
            System.out.println("Query from index");
        }
        else{
            result = this.fullCollectionSearch(operations, match_all);
            System.out.println("Full collection search");
        }

        System.out.println("######################################################");
        System.out.println("Operations: " + operations);
        System.out.println("Result: " + result);
        System.out.println("######################################################");

        return result;
    }

    private ArrayList<Document> fullCollectionSearch(ArrayList<Operation> operations, boolean match_all){
        /* Do full collection search for not indexed fields */
        // operations ar ORed together

        DBController db_controller = DBController.getDbController();
        Collection collection = db_controller.getDatabase(this.db_name).getCollection(this.collection_name);
        CollectionFileController file_controller = new CollectionFileController(collection.getController().getPath());
        JSONArray data = new JSONArray();
        try{
            data = file_controller.getAllDocuments();
        } catch (IOException | ParseException ignored){}

        Set<String> result_ids = new HashSet<>();
        ArrayList<Document> result = new ArrayList<>();

        for (Object o: data){

            JSONObject document_data = (JSONObject) o;
            String doc_id = (String) document_data.get("_id");
            Document document = new Document(doc_id, document_data.toJSONString());
            int operations_passed = 0;
            for (Operation operation: operations){
                switch (operation.type) {
                    case EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                        BinaryOperation binary_operation = (BinaryOperation) operation;
                        String field_name = binary_operation.operand1;
                        Object value = document_data.get(field_name);
                        if (binary_operation.apply(value)){
                            operations_passed++;
                        }
                    }
                    case IN -> {}
                    case LIKE -> {}
                    case IS_NULL -> {}
                }
            }
            if (match_all && operations_passed == operations.size()){
                if (result_ids.add(doc_id)){
                    result.add(document);
                }
            }
            else if (!match_all && operations_passed >= 1){
                if (result_ids.add(doc_id)){
                    result.add(document);
                }
            }
        }

        return result;
    }




}
