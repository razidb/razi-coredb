package auth;

import acl.AclUtils;
import acl.Permissions;

//import javax.servlet.http.HttpServletRequest;
//
//public class AuthRequestManager {
//    public static boolean isAuthorized(AuthController authController, HttpServletRequest request, String resourcePath){
//        int resourceInode = AclUtils.getResourcePathInode(resourcePath);
//        String authToken = request.getHeader("Authorization");
//        if (authToken == null)
//            return false;
//        else{
//            if (authToken.matches("(?i)jwt .*")){
//                // TODO: get user from jwt token
//                User authUser = authController.getUserByUsername("admin");
//                boolean isAuthenticated = authController.validateJwtToken(authToken.split(" ")[1]);
//                if (isAuthenticated){
////                    isAuthenticated = authController.acl.checkPermission(resourceInode, Permissions.READ, authUser);
//                }
//
//                return isAuthenticated;
//            }
//            else{
//                return false;
//            }
//        }
//    }
//}
