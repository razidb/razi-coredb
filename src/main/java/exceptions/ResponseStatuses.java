package exceptions;

public enum ResponseStatuses {
    SUCCESS(200),
    UNAUTHORIZED(300),
    FAILED(400);

    private int code;
    ResponseStatuses(int code){
        this.code = code;
    }
}
