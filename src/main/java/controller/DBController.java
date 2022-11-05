package controller;

import database.Database;

import java.io.File;
import java.util.Objects;
import java.util.Vector;

public class DBController {
    Vector<Database> databases;
    private static DBController dbController = null;

    private DBController(){
        // load all databases
        databases = new Vector<>();
        System.out.println("Loading Databases...");
        this.loadDatabase();
        System.out.println("Database is up and running");

    }

    public static DBController getDbController(){
        if (dbController == null)
            dbController = new DBController();
        return dbController;
    }

    public boolean loadDatabase(){
        this.databases.clear();
        String dataPath = "./db/data/";
        File dataDir = new File(dataPath);
        for (File file: Objects.requireNonNull(dataDir.listFiles())){
            Database db = new Database(file.getName());
            databases.add(db);
        }
        return true;
    }

    public Database getDatabase(String name){
        // return database by its name
        for (Database database: databases){
            if (Objects.equals(database.getName(), name))
                return database;
        }
        return null;
    }

}
