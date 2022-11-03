package org.example;

import java.net.Socket;

public class ConnectionHandler extends Thread {
    private Socket client;
    private boolean exit;

    ConnectionHandler(Socket client){
        client = client;
        // this msg will be printed only when a client is connected
        System.out.println("connect with client on " + client);
        exit = false;
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
