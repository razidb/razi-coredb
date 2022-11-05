package acl;

import auth.User;

import java.util.ArrayList;
import java.util.Objects;

public class Permission {
    String name;
    ArrayList<User> users;

    public Permission(String name, ArrayList<User> users){
        this.name = name;
        this.users = users;
    }

    public User getUser(String userId){
        for (User user: users){
            if(Objects.equals(user.get_id(), userId))
                return user;
        }
        return null;
    }
}
