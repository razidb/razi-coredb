package queryParserLayer.operations;

public class BaseOperation {
    Operations type;
    String[] args;

    BaseOperation(Operations type){
        this.type = type;
    }

    public void setArgs(String[] args) {
        this.args = args;
    }
}
