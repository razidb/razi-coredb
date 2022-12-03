package network;

import queryParserLayer.operations.MainOperations;

import java.io.IOException;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;

public class SocketInputStreamHandler {
    // ############################## Application Layer ##############################
    // NOTE: byte order (CPU’s endianness) is important when reading bytes
    // The format of the data (Type Length Value protocol)
    // ------------------------------------------------------------------------------
    // | content type | operation type | content length |            MSG             |
    // ^--- 1 Byte ---^---- 1 Byte ----^--- 4 Bytes ----^----------------------------

    private final DataInputStream inputStream;
    private String dataType;
    private int dataLength;

    private MainOperations operationType;
    private String data;

    public SocketInputStreamHandler(DataInputStream inputStream){
        this.inputStream = inputStream;
    }

    // Input handlers
    public void handle() throws IOException {

        // for we will handle only string data/payload
        char dataType = this.inputStream.readChar();
        char operation = this.inputStream.readChar();
        int length = this.inputStream.readInt();

        this.handleOperation(operation);
        this.handleDataLength(length);

        byte[] messageByte = new byte[length];
        boolean end = false;
        StringBuilder dataString = new StringBuilder(length);
        int totalBytesRead = 0;

        while(!end) {
            int currentBytesRead = this.inputStream.read(messageByte);
            totalBytesRead = currentBytesRead + totalBytesRead;
            if(totalBytesRead <= length) {
                dataString.append(new String(messageByte, 0, currentBytesRead, StandardCharsets.UTF_8));
            } else {
                dataString.append(
                        new String(
                                messageByte,
                                0,
                                length - totalBytesRead + currentBytesRead,
                                StandardCharsets.UTF_8
                        )
                );
            }
            if(dataString.length()>=length) {
                end = true;
            }
        }
        this.data = String.valueOf(dataString);
    }
    private void handleOperation(char operation){
        this.dataType = String.valueOf(dataType);
        switch (operation){
            case 's':
                this.operationType = MainOperations.SELECT;
                break;
            case 'i':
                this.operationType = MainOperations.INSERT;
                break;
            case 'u':
                this.operationType = MainOperations.UPDATE;
                break;
            case 'd':
                this.operationType = MainOperations.DELETE;
                break;
            case 'e':
                this.operationType = MainOperations.END;
                break;
            default: {
                // TODO: raise exception, unrecognized operation
                break;
            }
        }
    }
    private void handleDataLength(int length){
        this.dataLength = length;
    }

    // getter methods
    public String getDataType() {
        return dataType;
    }
    public int getDataLength() {
        return dataLength;
    }
    public MainOperations getOperationType() {
        return operationType;
    }
    public String getData() {
        return data;
    }
}
