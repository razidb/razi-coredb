package acl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

public class AclUtils {
    public static int getResourcePathInode(String resourcePath){
        try{
            Path file = Path.of(resourcePath);
            BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
            String s = attr.fileKey().toString();
            String inode = s.substring(s.indexOf("ino=") + 4, s.indexOf(")"));
            return Integer.parseInt(inode);
        }
        catch (Exception e){
            return 0;
        }

    }
}
