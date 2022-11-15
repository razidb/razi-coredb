package queryParserLayer.clauses;


public class Operation {
    public final Operations type;
    public final Boolean not;

    public Operation(Operations type, Boolean not) {
        this.type = type;
        this.not = not;
    }
}
