package queryParserLayer.operations;

public class BaseOperation {
    MainOperations type;
    String[] args;

    BaseOperation(MainOperations type){
        this.type = type;
    }

    public void setArgs(String[] args) {
        this.args = args;
    }
}
