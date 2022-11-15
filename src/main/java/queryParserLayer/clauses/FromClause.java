package queryParserLayer.clauses;

public class FromClause extends BaseClause{
    String from;
    public FromClause() {
        super(Clauses.FROM);
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }
}
