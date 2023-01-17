package shell;

import auth.AuthController;
import auth.AuthManager;
import controller.DBController;
import dataLayer.Resolver;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import queryParserLayer.operations.MainOperations;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Command(name = "razidb", version = "0.01", mixinStandardHelpOptions = true)
public class Shell implements Runnable{

    @Option(names = { "-u", "--username" }, description = "Username", required = true)
    String username;
    @Option(names = { "-p", "--password" }, description = "Password", required = true)
    String password;

    String database = "test";
    String collection = "test";

    public static AuthManager authManager;
    public static DBController dbController;
    public static AuthController authController;

    public Shell(){
        // auth managers on server initialized
        authController = AuthController.getAuthController();
        authManager = AuthManager.getAuthManager();
    }

    @Override
    public void run() {
        // make sure user is authenticated
        boolean authenticated = authenticate();
        if (authenticated)
            System.out.println("user authenticated successfully");
        else
            System.exit(400);

        // load data after authentication
        dbController = DBController.getDbController();
        // init JSON parser
        JSONParser jsonParser = new JSONParser();
        Scanner sc= new Scanner(System.in);

        while (true){
            // get user input
            String s = sc.nextLine();
            // parse user input
            // [database].[collection].[all/find/count][({query})] / [insertOne/insertMany/update/delete][(data)]
            String[] command = s.split("\\.");
            database = command[0];
            collection = command[1];

            String finalPart = command[2];
            String[] parts = finalPart.split("\\(");
            String operation = parts[0];
            String query = parts[1].split("\\)").length > 0? parts[1].split("\\)")[0]: null;

            // log query
            System.out.println("Query: " + query);

            MainOperations mainOperation = null;
            JSONObject jsonQuery = new JSONObject();

            try{
                jsonQuery = (JSONObject) jsonParser.parse(query);
            } catch (Exception e){
                System.out.println("Error while parsing query");
                System.out.println(e.getMessage());
            }

            // Selection Operation Related Queries
            if (Objects.equals(operation, "all") || Objects.equals(operation, "find")){
                mainOperation = MainOperations.SELECT;
            }
            else if (Objects.equals(operation, "insertOne") || Objects.equals(operation, "insertMany")){
                mainOperation = MainOperations.INSERT;
                JSONObject payload = new JSONObject();
                payload.put("type", "document");
                payload.put("payload", jsonQuery);
                jsonQuery = payload;
            }
            else if (Objects.equals(operation, "update")){
                mainOperation = MainOperations.UPDATE;
                JSONObject payload = new JSONObject();
                payload.put("type", "document");
                payload.put("payload", jsonQuery);
                jsonQuery = payload;
            }
            else if (Objects.equals(operation, "delete")){
                mainOperation = MainOperations.DELETE;
                JSONObject payload = new JSONObject();
                payload.put("type", "document");
                payload.put("payload", jsonQuery);
                jsonQuery = payload;
            }
            // index-related queries
            else if (Objects.equals(operation, "createIndex")) {
                mainOperation = MainOperations.INSERT;
                JSONObject payload = new JSONObject();
                payload.put("type", "index");
                payload.put("payload", jsonQuery.get("fields"));
                jsonQuery = payload;
            }
            else if (Objects.equals(operation, "deleteIndex")) {
                mainOperation = MainOperations.DELETE;
                JSONObject payload = new JSONObject();
                payload.put("type", "index");
                payload.put("payload", jsonQuery.get("fields"));
                jsonQuery = payload;
            }

            Resolver resolver = new Resolver(database, collection, mainOperation);
            resolver.setQuery(jsonQuery);
            try {
                resolver.resolve();
            } catch (Exception e) {
                System.out.println("Something went wrong");
                // System.out.println(e.getMessage());
                throw new RuntimeException(e);

            }
            System.out.println(resolver.getQueryset());

        }

    }

    private boolean authenticate(){
        boolean authenticated = authController.authenticate(username, password);
        System.out.println("authenticating user: " + username);
        return authenticated;
    }


    public static void main(String[] args) {
        int exitCode = new CommandLine(new Shell()).execute(args);

        if (false){
            try{
                List<String> records = Files.readAllLines(Path.of("db/data/test2/test/data.txt"));
                for (int i=1; i<=1000000; i++){
                    Map<String, Object> record = new HashMap<>();

                    record.put("_id", i);
                    record.put("username", "test" + i);
                    record.put("firstName", "FirstName" + i);
                    record.put("lastName", "LastName" + i);
                    record.put("gender", i%2 == 0? "m": "f");
                    record.put("faculty", "KASIT");
                    record.put("department", i%8 == 0? "cs": i%10 == 0? "cis": "bit");
                    record.put("creditHours", i%135);
                    record.put("gpa", i%5);
                    record.put("year", 2000 + i%23);
                    record.put("status", i%4 == 0? "A": "I");

                    JSONObject jsonRecord = new JSONObject(record);
                    records.add(jsonRecord.toString());

                }
                Files.write(Path.of("db/data/test2/test/data.txt"), records);
            } catch (Exception e){
                System.out.println(e.getMessage());
            }
        }


        System.exit(exitCode);
    }
}
