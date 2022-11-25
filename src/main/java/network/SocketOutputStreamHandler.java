package network;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SocketOutputStreamHandler {

    // Important information for the response
    // -----------------------------------------------------------------------
    // | content type | content length | status code |         response       |
    // ^--- 1 Byte ---^--- 4 Bytes ----^-- 4 Bytes --^------------------------

    private DataOutputStream dataOutputStream;
    private final char contentType = 's';
    private int statusCode;
    private String content;

    public SocketOutputStreamHandler(DataOutputStream dataOutputStream, int codeStatus, String content){
        this.dataOutputStream = dataOutputStream;
        this.content = content;
        this.statusCode = codeStatus;
    }

    public void handle() throws IOException {
        byte[] contentInBytes = this.content.getBytes(StandardCharsets.UTF_8);

        this.dataOutputStream.writeChar(contentType);
        this.dataOutputStream.writeInt(contentInBytes.length);
        this.dataOutputStream.writeInt(statusCode);
        this.dataOutputStream.write(contentInBytes);
        this.dataOutputStream.flush();
    }

}
