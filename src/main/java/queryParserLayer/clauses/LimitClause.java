package queryParserLayer.clauses;

public class LimitClause extends BaseClause{
    Integer limit;
    LimitClause(Clauses type, Integer limit) {
        super(type);
        this.limit = limit;
    }
}
