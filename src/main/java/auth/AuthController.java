package auth;

import acl.ACL;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;


public class AuthController {

    ACL acl;
    ArrayList<User> users;
    private static String SECRET_KEY = "oeRaYY7Wo24sDqKSX3IM9ASGmdGPmkTd9jo1QTy4b7P9Ze5_9hKolVX8xNrQDcNRfVEdTZNOuOyqEGhXEbdJI-ZQ19k_o9MI0y3eZN2lp9jow55FfXMiINEdt1XR85VipRLSOkT6kSpzs2x-jbLDiz9iFVzkd81YKxMgPA7VfZeQUm4n-mOmnWMaVX30zGFU4L3oPBctYKkl4dYfqYWqRNfrgPJVi5DGFjywgxx0ASEiJHtV72paI3fDR2XwlSkyhhmY-ICjCRmsJN4fX1pdoL8a18-aQrvyu4j0Os6dVPYIoPvvY0SAZtWYKHfM15g7A3HD4cVREf9cUsprCRK93w";

    private static AuthController authController;

    private AuthController(){
        this.users = new ArrayList<>();
        loadUsers();
        this.acl = new ACL(this);

    }

    public static AuthController getAuthController(){
        if (authController == null)
            authController = new AuthController();
        return authController;
    }

    public ACL getAcl() {
        return acl;
    }

    private void loadUsers(){
        JSONArray usersFromFile = null;
        try{
            usersFromFile = AuthFileController.getAllDocuments();
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
        for (Object userFromFile: usersFromFile){
            JSONObject userObjectFromFile = (JSONObject) userFromFile;
            String userId = userObjectFromFile.getOrDefault("_id", "").toString();
            String username = userObjectFromFile.getOrDefault("username", "").toString();
            String password = userObjectFromFile.getOrDefault("password", "").toString();
            JSONArray rolesFromFile = (JSONArray) userObjectFromFile.getOrDefault("roles", null);
            ArrayList<String> roles = new ArrayList<>();
            for (Object roleFromFile: rolesFromFile){
                roles.add(roleFromFile.toString());
            }
            User user = new User(userId, username, password, roles);
            this.users.add(user);
        }
    }

    public User getUserByUsername(String username){
        for (User user: users){
            if (Objects.equals(user.getUsername(), username))
                return user;
        }
        return null;
    }

    public User getUserById(String userId){
        for (User user: users){
            if (Objects.equals(user.get_id(), userId))
                return user;
        }
        return null;
    }

    public boolean authenticate(String username, String password){
        User user = getUserByUsername(username);
        if (user == null){
            throw new RuntimeException("User with username " + username + " does not exist");
        }

        return user.authenticate(password);
    }

    public String getToken(String username, String password){
        boolean isAuthenticated = authenticate(username, password);
        if (!isAuthenticated){
            throw new RuntimeException("Provide valid credentials");
        }
        User user = getUserByUsername(username);
        return createToken(user);
    }

    public static boolean validateJwtToken(String token){
        try {
//            decodeJWT(token);
            return true;
        }
        catch (Exception e){
            return false;
        }
    }

    private static String createToken(User user){
//        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;
//        long nowMillis = System.currentTimeMillis();
//        Date now = new Date(nowMillis);
//
//        //We will sign our JWT with our ApiKey secret
//        byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary(SECRET_KEY);
//        Key signingKey = new SecretKeySpec(apiKeySecretBytes, signatureAlgorithm.getJcaName());
//
//        //Let's set the JWT Claims
//        JwtBuilder builder = Jwts.builder()
//                .setIssuedAt(now)
//                .claim("user", user)
//                .signWith(signatureAlgorithm, signingKey);
//        long expireAfterMillis = 60*60*1000;
//        long expMillis = nowMillis + expireAfterMillis;
//        Date exp = new Date(expMillis);
//        builder.setExpiration(exp);
//
//        //Builds the JWT and serializes it to a compact, URL-safe string
//        return builder.compact();
        return "";
    }
//    public static Claims decodeJWT(String jwt) {
//        //This line will throw an exception if it is not a signed JWS (as expected)
//        Claims claims = Jwts.parser()
//                .setSigningKey(DatatypeConverter.parseBase64Binary(SECRET_KEY))
//                .parseClaimsJws(jwt).getBody();
//        return claims;
//    }
}
