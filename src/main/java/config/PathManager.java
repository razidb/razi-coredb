package config;

public class PathManager {

    public static String getDatabasePath(String dbName){
        return Configuration.DATA_PATH + dbName + "/";
    }

    public static String getCollectionPath(String dbName, String collectionName){
        return getDatabasePath(dbName) + collectionName + ".json";
    }
}
