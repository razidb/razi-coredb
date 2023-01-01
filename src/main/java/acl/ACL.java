package acl;

import auth.AuthController;
import auth.User;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class ACL {

    AuthController authController;
    ArrayList<ACLEntry> aclEntries;

    public ACL(AuthController authController){
        this.authController = authController;
        this.aclEntries = new ArrayList<>();
        // load data from file
        try{
            this.loadDataFromFile();
        }
        catch (Exception e){
            System.out.println("Failed to load ACL authorization");
            throw new RuntimeException(e);
        }
    }
    private void loadDataFromFile() throws IOException, ParseException {
        String aclFilePath = "./db/system/acl.json";

        JSONParser parser = new JSONParser();
        JSONObject aclObjFromFile = (JSONObject) parser.parse(new FileReader(aclFilePath));
        JSONArray aclEntriesFromFile = (JSONArray) aclObjFromFile.get("accessControlEntries");

        for (Object aclEntryFromFile: aclEntriesFromFile){
            // prepare data
            JSONObject aclEntryObjFromFile = (JSONObject) aclEntryFromFile;
            JSONObject resourceDetailsFromFile = (JSONObject) aclEntryObjFromFile.get("resource");
            JSONObject ownerDetailsFromFile = (JSONObject) aclEntryObjFromFile.get("owner");
            JSONArray permissionsDetailsFromFile = (JSONArray) aclEntryObjFromFile.get("permissions");

            String resourceId = resourceDetailsFromFile.get("id").toString();
            String resourceType = resourceDetailsFromFile.get("type").toString();
            ArrayList<Permission> permissions = new ArrayList<>();
            String ownerId = ownerDetailsFromFile.get("id").toString();
            User owner = this.authController.getUserById(ownerId);

            for(Object permissionFromFile: permissionsDetailsFromFile){
                String permissionName = ((JSONObject) permissionFromFile).get("permission").toString();
                JSONArray permit = (JSONArray) ((JSONObject) permissionFromFile).get("permit");
                ArrayList<User> users = new ArrayList<>();
                for(Object userId: permit){
                    User permittedUser = authController.getUserById(userId.toString());
                    if (permittedUser != null)
                        users.add(permittedUser);
                    else
                        System.out.println("user with id: " + userId + " does not exist");
                }
                Permission permission = new Permission(
                    permissionName, users
                );
                permissions.add(permission);
            }

            ACLEntry aclEntry = new ACLEntry(
                resourceId,
                resourceType,
                owner,
                permissions
            );
            this.aclEntries.add(aclEntry);
        }
    }

    public boolean checkPermission(String resourceId, Permissions p, User user){
        for (ACLEntry aclEntry: this.aclEntries){
            if(Objects.equals(aclEntry.getResourceId(), resourceId)){
                return aclEntry.checkPermission(p, user);
            }
        }
        return false;
    }
}
