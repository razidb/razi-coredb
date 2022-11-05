package auth;

import java.util.List;
import java.util.Objects;

public class AuthManager {

    private static AuthManager authManager;
    List<User> users;

    private AuthManager(){
        loadUsers();
        authManager = this;
    }

    public static AuthManager getAuthManager(){
        if (authManager == null){
            authManager = new AuthManager();
        }
        return authManager;
    }

    void loadUsers(){
        // TODO: implement functionality
        // load users from the database
    }

    User getUserByUsername(String username){
        for (User user: users){
            if (Objects.equals(user.getUsername(), username))
                return user;
        }
        return null;
    }

    public boolean authenticate(String username, String password){
        User user = getUserByUsername(username);
        if (user == null){
            // user does not exist
            return false;
        }
        return user.authenticate(password);
    }

    public boolean canPerformOperation(User user){
        boolean canPerformOperation = !user.shouldChangePassword();
        return canPerformOperation;
    }

}
