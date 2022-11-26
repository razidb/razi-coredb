package dataLayer;

import java.util.Set;
import java.util.Stack;
import java.util.HashSet;
import java.io.IOException;
import java.util.ArrayList;

import collection.Collection;
import controller.DBController;
import document.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import queryParserLayer.ParserResult;
import queryParserLayer.clauses.Operation;
import collection.CollectionFileController;
import queryParserLayer.clauses.Operations;
import org.json.simple.parser.ParseException;
import queryParserLayer.clauses.BinaryOperation;
import queryParserLayer.operations.BaseOperation;
import queryParserLayer.operations.MainOperations;
import queryParserLayer.operations.SelectOperation;


public class Resolver {
    private SelectionExecutor selection_executor;

    private final MainOperations operation;
    private JSONObject query;

    private int codeStatus;
    private Object result;

    public Resolver(String db, String collection, MainOperations operation){
        this.selection_executor = new SelectionExecutor(db, collection);
        this.operation = operation;
        this.result = new ArrayList<>();
    }

    public Object getResult() {
        return result;
    }

    public int getCodeStatus() {
        // TODO: get code status after executing query
        return 200;
    }

    public void setQuery(JSONObject query) {
        this.query = query;
    }

    public void resolve(){
        /*  Responsible for resolving the query  */

        // TODO: check the type of operation: Create, Read, Update, or Delete operation
        // TODO: use SelectOperation class and other classes to do so

        if (operation == MainOperations.SELECT){
            this.handleSelection();
        }
        else if (operation == MainOperations.CREATE){
            this.handleCreation();
        }
        else if (operation == MainOperations.UPDATE){
            this.handleUpdate();
        }
        else if (operation == MainOperations.DELETE){
            this.handleDeletion();
        }
        else {
            // operation is not valid

        }

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

        if (operations == null){
            operations = new ArrayList<>();
        }
        QuerySet result = null;
        Object top = operationStack.pop();

        if (top instanceof String && top.equals("$AND")){
            // init query result
            QuerySet query_result = new QuerySet(null);
            while (operationStack.peek() != "END $AND"){
                QuerySet querySet = resolveOperationStack(
                    operationStack, top, operations
                );
                if (querySet != null){
                    query_result.intersect(querySet);
                }
            }
            // popping element marking the end of the $AND operation
            operationStack.pop();

            // fetch data using a query from operations list
            ArrayList<Document> documents = null;
            if (operations.size() > 0){
                documents = this.selection_executor.execute(operations, true);
            }
            QuerySet operations_query_set = new QuerySet(documents);
            query_result.intersect(operations_query_set);
            result = query_result;
            // empty operations because we have fetch the data
            operations.clear();
        }
        else if (top instanceof String && top.equals("$OR")){
            QuerySet query_result = new QuerySet(null);
            // iterate over
            while (operationStack.peek() != "END $OR"){
                QuerySet querySet = resolveOperationStack(
                    operationStack, top, null
                );
                if (querySet != null){
                    query_result.merge(querySet);
                }
            }
            // pop the element marking the end of the $OR operation
            operationStack.pop();
            // Merge queries
            result = query_result;
        }
        else if (top instanceof Operation){
            // add the current operation to list of operations and return.
            operations.add((Operation) top);
        }

        return result;
    }

    public void handleSelection(){
        /* Facade method to handle select operation */
        // build operation stack
        Stack<Object> operation_stack = new Stack<>();
        this.buildOperationStack(operation_stack, query, "$AND", null);
        // resolve operation stack and get a query set
        QuerySet querySet = this.resolveOperationStack(operation_stack, null, new ArrayList<>());
        this.result = querySet.documents;
    }

    private void handleCreation(){
        /* Facade method to handle create operation */
    }

    private void handleUpdate(){
        /* Facade method to handle update operation */
    }

    private void handleDeletion(){
        /* Facade method to handle delete operation */
    }
}
