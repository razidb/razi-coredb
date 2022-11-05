package acl;

import auth.User;

import java.util.ArrayList;
import java.util.Objects;

public class ACLEntry {
    int resourceInode;
    String resourceType;

    User owner;

    ArrayList<Permission> permissions;

    public ACLEntry(int resourceInode, String resourceType, User owner, ArrayList<Permission> permissions){
        this.resourceInode = resourceInode;
        this.resourceType = resourceType;
        this.owner = owner;
        this.permissions = permissions;
    }

    public int getResourceInode() {
        return resourceInode;
    }


    public boolean permit(Permissions permission, User user){
        // add data to acl file

        return true;
    }

    public boolean checkPermission(Permissions p, User user){
        for (Permission permission: this.permissions){
            if (Objects.equals(permission.name, p.name()) && permission.getUser(user.get_id()) != null)
                return true;
        }
        return false;
    }
}
