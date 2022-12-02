package org.example;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;

import dataLayer.Resolver;
import document.Document;
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

    ConnectionHandler(Socket client) throws SocketException {
        // this msg will be printed only when a client is connected
        System.out.println("connect with client on " + client);
        this.exit = false;
        this.client = client;
        this.client.setKeepAlive(true);
        this.client.setSoTimeout(1000); // in milliseconds
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

        this.bufferedInputStream = new BufferedInputStream(this.client.getInputStream());
        this.dataInputStream = new DataInputStream(bufferedInputStream);
        this.dataOutputStream = new DataOutputStream(this.client.getOutputStream());

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

            String dataString = socketInputStreamHandler.getData();
            // use json parser to parse json-string to json-object
            JSONParser jsonParser = new JSONParser();
            JSONObject query = (JSONObject) jsonParser.parse(String.valueOf(dataString));

            // log received query
            System.out.println("Received Query: " + query);

            // TODO: get database name and collection from request
            String db_name = "test";
            String collection_name = "test";

            // query resolver
            Resolver resolver = new Resolver(db_name, collection_name, operationType);
            resolver.setQuery(query);
            resolver.resolve();
            // code status
            int codeStatus = resolver.getCodeStatus();

            // log execution time
            // TODO: create a logger and log query time

            // get query result
            Object query_result = resolver.getResult();

            // send result to client
            SocketOutputStreamHandler socketOutputStreamHandler = new SocketOutputStreamHandler(
                this.dataOutputStream, codeStatus, query_result.toString()
            );
            socketOutputStreamHandler.handle();
        }
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
