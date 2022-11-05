package auth;

import java.util.List;
import java.util.Objects;

public class User {

    String _id;
    String username;
    String password;
    List<String> roles;


    public User(String _id, String username, String password, List<String> roles){
        this._id = _id;
        this.username = username;
        this.roles = roles;
        this.password = password;
    }

    public String get_id() {
        return _id;
    }

    public String getUsername() {
        return username;
    }

    public List<String> getRoles() {
        return roles;
    }

    public boolean authenticate(String password){
        return Objects.equals(this.password, password);
    }

    public void changePassword(String oldPassword, String newPassword){
        boolean isAuthenticated = authenticate(oldPassword);
        if (isAuthenticated){
            // allow change password here
        }
    }

    public boolean shouldChangePassword(){
        return Objects.equals(password, "default");
    }
}
