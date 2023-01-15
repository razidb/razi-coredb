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

import java.util.Objects;
import java.util.Scanner;

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

            System.out.println(query);

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


            Resolver resolver = new Resolver(database, collection, mainOperation);
            resolver.setQuery(jsonQuery);
            resolver.resolve();
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
        System.exit(exitCode);
    }
}
