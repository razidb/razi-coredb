package org.example;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;

import acl.Permission;
import acl.Permissions;
import auth.AuthController;
import auth.User;
import dataLayer.Resolver;
import network.SocketOutputStreamHandler;
import org.json.simple.JSONObject;
import network.SocketInputStreamHandler;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import queryParserLayer.operations.MainOperations;


public class ConnectionHandler extends Thread {
    /*
    * To establish a session with a client:
    * 1- Send a request-to-connect by providing username and password.
    * 2- Successfully authenticate the user.
    * 3- Start performing queries.
    * 4- End the connection.
    * */
    private Socket client;
    private boolean exit;
    // input and output streams
    private BufferedInputStream bufferedInputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    // json parser
    private JSONParser jsonParser;
    // user data
    private User user;

    ConnectionHandler(Socket client) throws IOException {
        // this msg will be printed only when a client is connected
        System.out.println("connect with client on " + client);
        this.exit = false;
        this.client = client;
        this.client.setKeepAlive(true);
        this.client.setSoTimeout(1000); // in milliseconds

        this.bufferedInputStream = new BufferedInputStream(this.client.getInputStream());
        this.dataInputStream = new DataInputStream(bufferedInputStream);
        this.dataOutputStream = new DataOutputStream(client.getOutputStream());
        // use json parser to parse json-string to json-object
        this.jsonParser = new JSONParser();
    }

    @Override
    public void run() {
        System.out.println("Start running...");
        try {
            this.handle();
        } catch (IOException | ParseException e) {
            System.out.println("Error: " + e.getMessage());
            System.out.println("Stack: " + Arrays.toString(e.getStackTrace()));
        }
    }

    private void handle() throws IOException, ParseException {
        // 1- REQUEST-TO-CONNECT
        this.handleRequestToConnect();
        // 2- Start Performing Queries

        // The purpose of the while loop is to keep connection open
        // with client until exit command is fired or timeout
        while (!exit){
            SocketInputStreamHandler socketInputStreamHandler = new SocketInputStreamHandler(dataInputStream);
            socketInputStreamHandler.handle();

            MainOperations operationType = socketInputStreamHandler.getOperationType();

            // end connection when operation is END
            if (MainOperations.END.equals(operationType)){
                this.end();
                break;
            }

            // TODO: get database name and collection from request
            String db_name = "test";
            String collection_name = "test";
            String collectionId = "281deedc-728a-4355-a255-a8b52bd4190d";

            AuthController authController = AuthController.getAuthController();
            Permissions permission = (
                operationType == MainOperations.DELETE
                || operationType == MainOperations.INSERT
                || operationType == MainOperations.UPDATE ? Permissions.WRITE : Permissions.READ
            );
            boolean authorized = authController.getAcl().checkPermission(
                collectionId, permission, this.user
            );

            int codeStatus = 0;
            Object query_result = null;

            if (authorized){
                String dataString = socketInputStreamHandler.getData();
                JSONObject query = (JSONObject) jsonParser.parse(String.valueOf(dataString));

                // log received query
                System.out.println("Received Query: " + query);

                // query resolver
                Resolver resolver = new Resolver(db_name, collection_name, operationType);
                resolver.setQuery(query);
                resolver.resolve();

                // code status
                codeStatus = resolver.getCodeStatus();

                // log execution time
                // TODO: create a logger and log query time

                // get query result
                query_result = resolver.getResult();

            }
            else{
                // ignore query and return authorization error
                codeStatus = 400;
                query_result = "unauthorized";
            }

            // send result to client
            SocketOutputStreamHandler socketOutputStreamHandler = new SocketOutputStreamHandler(
                this.dataOutputStream, codeStatus, query_result.toString()
            );
            socketOutputStreamHandler.handle();
        }
    }

    private void handleRequestToConnect() throws IOException, ParseException {
        // This is the beginning request in a chain-of-requests to open a connection
        // If this fails, the connection will be terminated, if succeeded, a session is established.
        // Expected Request Content:
        // - username (required)
        // - password (required)
        // - database (optional)

        // Expected Response:
        // - successful authentication (ok)
        //      -> create a session
        // - failed to authenticate (error)

        SocketInputStreamHandler initSocketInputStreamHandler = new SocketInputStreamHandler(dataInputStream);
        initSocketInputStreamHandler.handle();
        // get data from request
        String strAuthData = initSocketInputStreamHandler.getData();
        JSONObject authData = (JSONObject) jsonParser.parse(String.valueOf(strAuthData));
        String username = (String) authData.getOrDefault("username", "default");
        String password = (String) authData.getOrDefault("password", null);
        // authenticate
        AuthController authController = AuthController.getAuthController();
        boolean isAuthenticated = authController.authenticate(username, password);
        // set response status code
        int statusCode = isAuthenticated? 200: 401;
        // send result to client
        SocketOutputStreamHandler initSocketOutputStreamHandler = new SocketOutputStreamHandler(
                this.dataOutputStream, statusCode, isAuthenticated? "ok": "unauthorized"
        );
        initSocketOutputStreamHandler.handle();
        // end the connection if authentication failed
        if (!isAuthenticated)
            end();

        this.user = authController.getUserByUsername(username);

    }

    private void end() throws IOException {
        System.out.println("Thread terminated, client " + client);
        this.exit = true;
        // close resources
        this.client.close();
        this.bufferedInputStream.close();
        this.dataInputStream.close();
    }
}
