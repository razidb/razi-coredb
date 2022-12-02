package org.example;

import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import auth.AuthManager;
import auth.AuthController;
import controller.DBController;
import org.json.simple.JSONArray;


public class Server{
    /*
    *
    * Main database access point, responsible for receiving initial
    * clients connections.
    * Making sure data is loaded initially when the server is initiated.
    *
    * */

    // leave port number > 1023 as linux reserve ports less than 1023
    private static final int PORT = 4999;
    private static ServerSocket server;
    public static AuthManager authManager;
    public static DBController dbController;
    public static AuthController authController;

    private static Logger logger;

    public static void main(String[] args) throws IOException {
        // load db and auth managers on server initialized
        authManager = AuthManager.getAuthManager();
        dbController = DBController.getDbController();
        authController = AuthController.getAuthController();
        logger = Logger.getLogger(Server.class.getName());
        /*
        // Adding 1 million records in the database
        JSONArray data = new JSONArray();
        for (int i=1; i< 1000*1000; i++){
            JSONObject object = new JSONObject();
            object.put("_id", UUID.randomUUID().toString());
            object.put("firstName", "test" + i);
            object.put("lastName", "test" + i);
            object.put("username", "test" + i);
            object.put("status", i%2 == 0?"A":"I");
            data.add(object);
        }

        dbController.getDatabase("test").getCollection("test").getController().addDocumentsToCollection(data);
        */

        server = new ServerSocket(PORT);

        while (true){
            logger.log(Level.INFO, "Waiting for a client to connect....");
            handleConnection();
        }
    }

    private static void handleConnection(){
        try{
            ConnectionHandler dbConnectionHandler = new ConnectionHandler(server.accept());
            Thread thread = new Thread(dbConnectionHandler);
            thread.start();
        }
        catch (IOException e){
            logger.warning("Could not listen on port: " + PORT);
            logger.warning(e.getMessage());
            System.exit(-1);
        }
        catch (Exception syntaxException){
            logger.warning("Invalid syntax: " + syntaxException.getMessage());
            System.out.println(Arrays.toString(syntaxException.getStackTrace()));
        }
    }
}
