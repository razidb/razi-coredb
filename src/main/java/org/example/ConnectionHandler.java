package org.example;

import java.net.Socket;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.BufferedInputStream;

public class ConnectionHandler extends Thread {
    private Socket client;
    private boolean exit;

    ConnectionHandler(Socket client) throws IOException {
        this.client = client;
        // this msg will be printed only when a client is connected
        System.out.println("connect with client on " + client);
        exit = false;

        DataInputStream in = new DataInputStream(
            new BufferedInputStream(client.getInputStream())
        );

        System.out.println(
            Server
                .dbController
                .getDatabase("test")
                .getCollection("test")
                .find("username", "mahdyhamad")
        );

        // request -> Query Parser Layer -> Access Control Layer -> Data layer;
    }

    @Override
    public void run() {
        System.out.println("Start running ");
        while(!exit){
            for (int i=0; i<10; i++){
                System.out.println(i);
            }
            end();
        }
    }

    private void end(){
        System.out.println("Thread terminated");
        exit = true;
    }
}
