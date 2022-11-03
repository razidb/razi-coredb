package org.example;

import java.io.*;
import java.net.*;


public class Server{
    // leave port number > 1023 as linux reserve ports less than 1023
    private static int port = 4999;
    private static ServerSocket server;

    public static void main(String[] args) throws IOException {
        server = new ServerSocket(port);
        while (true){
            System.out.println("Waiting for a client to connect....");
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
            System.out.println("Could not listen on port: " + port);
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }
}
