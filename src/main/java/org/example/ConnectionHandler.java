package org.example;

import dataLayer.Resolver;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

import java.net.Socket;
import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.util.Stack;

public class ConnectionHandler extends Thread {
    private Socket client;
    private boolean exit;

    ConnectionHandler(Socket client) throws Exception {
        this.client = client;
        // this msg will be printed only when a client is connected
        System.out.println("connect with client on " + client);
        exit = false;

        DataInputStream in = new DataInputStream(
            new BufferedInputStream(client.getInputStream())
        );

        // TODO: get query from socket
        String str_query = "" +
                "{\"username\":\"mahdyhamad\", \"status\": \"A\", " +
                    "\"$OR\": [" +
                        "{\"firstName\":\"test1\", \"lastName\": \"test1\"}" +
//                        "{\"grade\":{\"$gte\": \"10\"}, \"lastName\": \"test2\"}" +
                    "]" +
                "}";

//        str_query = "{\"status\": \"A\", \"username\":\"test2\"}";
        // use json parser to parse json string to json object
        JSONParser jsonParser = new JSONParser();
        JSONObject query = (JSONObject) jsonParser.parse(str_query);
        System.out.println(query);

        // query resolver
        Resolver resolver = new Resolver("test", "test");
        resolver.setQuery(query);
        resolver.resolve();
        System.out.println(resolver.getDocuments());

    }

    @Override
    public void run() {
        System.out.println("Start running...");
        end();
    }

    private void end(){
        System.out.println("Thread terminated, client " + client);
        exit = true;
    }
}
