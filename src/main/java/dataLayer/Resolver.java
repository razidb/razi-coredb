package dataLayer;

import java.util.Set;
import java.util.Stack;
import java.util.HashSet;
import java.io.IOException;
import java.util.ArrayList;

import collection.Collection;
import controller.DBController;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import queryParserLayer.ParserResult;
import queryParserLayer.clauses.Operation;
import collection.CollectionFileController;
import queryParserLayer.clauses.Operations;
import org.json.simple.parser.ParseException;
import queryParserLayer.clauses.BinaryOperation;
import queryParserLayer.operations.BaseOperation;
import queryParserLayer.operations.SelectOperation;


public class Resolver {
    private final String db_name;
    private final String collection_name;
    private JSONObject query;
    private ArrayList<JSONObject> documents;


    public Resolver(String db, String collection){
        this.db_name = db;
        this.collection_name = collection;
        this.documents = new ArrayList<>();
    }

    public ArrayList<JSONObject> getDocuments() {
        return documents;
    }

    public void setQuery(JSONObject query) {
        this.query = query;
    }

    public void resolve(){
        /*  Responsible for resolving the query  */

        // TODO: check the type of operation: Create, Read, Update, or Delete operation
        // TODO: use SelectOperation class and other classes to do so

        // build operation stack
        Stack<Object> operation_stack = new Stack<>();
        this.buildOperationStack(operation_stack, query, "$AND", null);
        // resolve operation stack and get a query set
        QuerySet querySet = this.resolveOperationStack(operation_stack, null, new ArrayList<>());
        this.documents = querySet.documents;

    }

    private void buildOperationStack(
            Stack<Object> operation_stack,
            Object sub_query,
            String main_operation,
            String parent_field
    ){
        /*  Build the operation stack the contains a post-fix notation of the query  */

        String main_op = "";
        if (sub_query instanceof JSONObject){
            main_op = "$AND";
            // Make sure we know where this operation ends
            operation_stack.push("END $AND");
            // iterate over keys
            for (Object o: ((JSONObject) sub_query).keySet()){
                String key = (String) o;
                Object value = ((JSONObject) sub_query).get(key);

                // keyword query starts with $ (ex: $AND, $lte, ...)
                if (key.startsWith("$")){
                    if (key.equals("$OR")){
                        buildOperationStack(operation_stack, value, "$OR", null);
                    }
                    // ## this is probably not a valid query ##
                    else if (key.equals("$AND")) {
                        buildOperationStack(operation_stack, value, "$AND", null);
                    }
                    else{
                        // here what is left is lookups on a field (ex: $lt, $lte, ...)
                        String operand = (String) value;
                        Operations operation_type = null;
                        switch (key){
                            case "$lt" -> operation_type = Operations.LESS_THAN;
                            case "$lte" -> operation_type = Operations.LESS_THAN_OR_EQUAL;
                            case "$gt" -> operation_type = Operations.GREATER_THAN;
                            case "$gte" -> operation_type = Operations.GREATER_THAN_OR_EQUAL;
                        }
                        BinaryOperation binaryOperation = new BinaryOperation(
                                operation_type, false, parent_field, operand
                        );
                        operation_stack.push(binaryOperation);
                    }
                }
                else{
                    // this is either a normal field lookup or a nested field lookup
                    if (value instanceof JSONObject){
                        buildOperationStack(operation_stack, value, null, key);
                    }
                    else{
                        // normal field lookup
                        BinaryOperation equalOperation = new BinaryOperation(
                                Operations.EQUAL, false, key, (String) value
                        );
                        operation_stack.push(equalOperation);
                    }
                }
            }
        }
        else if (sub_query instanceof JSONArray sub_query_array){
            main_op = "$OR";
            // Make sure we know where this operation ends
            operation_stack.push("END $OR");
            for (Object obj_query: sub_query_array){
                // the reason behind main operation is $AND because every item in the $OR clause is an
                // object and all element in the object should be ANDed together
                buildOperationStack(operation_stack, obj_query, "$AND", null);
            }
        }

        if (main_operation != null){
            operation_stack.push(main_operation);
        }
    }

    private QuerySet resolveOperationStack(
        Stack<Object> operationStack,
        Object parent_operation,
        ArrayList<Operation> operations
    ){
        /*  Responsible for querying data in operations stack with the appropriate order  */

        QuerySet result = null;
        Object top = operationStack.pop();

        if (top instanceof String && top.equals("$AND")){
            // perform intersection between `query_set` later
            ArrayList<QuerySet> queries = new ArrayList<>();
            while (operationStack.peek() != "END $AND"){
                QuerySet querySet = resolveOperationStack(
                    operationStack, top, operations
                );
                if (querySet != null)
                    queries.add(querySet);
            }
            // pop the element marking the end of the $AND operation
            operationStack.pop();

            // fetch data using a query from operations list
            ArrayList<JSONObject> data = new ArrayList<>();
            if (operations.size() > 0)
                data = fullCollectionSearch(operations, true);
            QuerySet operations_query_set = new QuerySet(data);
            // perform intersection over queries
            // TODO: implement functionality
            ArrayList<JSONObject> d = new ArrayList<>();
            for(QuerySet querySet: queries){
                d.addAll(querySet.documents);
            }
            d.addAll(operations_query_set.documents);

            result = new QuerySet(d);

            // empty operations because we have fetch the data
            operations.clear();
        }
        else if (top instanceof String && top.equals("$OR")){
            // Merge queries resulting from $OR clause
            ArrayList<QuerySet> queries = new ArrayList<>();
            // iterate over
            while (operationStack.peek() != "END $OR"){
                QuerySet querySet = resolveOperationStack(
                    operationStack, top, operations
                );
                if (querySet != null)
                    queries.add(querySet);
            }
            // pop the element marking the end of the $OR operation
            operationStack.pop();

            // Merge queries
            // TODO: implement functionality
            ArrayList<JSONObject> data = new ArrayList<>();
            for(QuerySet querySet: queries){
                data.addAll(querySet.documents);
            }

            result = new QuerySet(data);
        }
        else if (top instanceof Operation){
            // add the current operation to list of operations and return.
            operations.add((Operation) top);
        }
        return result;
    }

    private ArrayList<JSONObject> fullCollectionSearch(ArrayList<Operation> operations, boolean match_all){
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
        ArrayList<JSONObject> result = new ArrayList<>();

        for (Object o: data){
            JSONObject document = (JSONObject) o;
            String doc_id = (String) document.get("_id");
            int operations_passed = 0;
            for (Operation operation: operations){
                switch (operation.type) {
                    case EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                        BinaryOperation binary_operation = (BinaryOperation) operation;
                        String field_name = binary_operation.operand1;
                        Object value = document.get(field_name);
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


    public void execute(ParserResult parserResult){
        BaseOperation operation = parserResult.getBaseOperation();
        DBController dbController = DBController.getDbController();
        if (operation instanceof SelectOperation){
            // TODO: get database used in the current session
            Collection collection = dbController
                    .getDatabase(this.db_name)
                    .getCollection(
                        ((SelectOperation) operation).getFromClause().getFrom()
                    );
            if (((SelectOperation) operation).getWhereClause() != null){
                // TODO: execute select query here
            }
        }
    }
}
