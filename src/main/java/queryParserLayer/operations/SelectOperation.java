package queryParserLayer.operations;

import queryParserLayer.clauses.FromClause;
import queryParserLayer.clauses.LimitClause;
import queryParserLayer.clauses.WhereClause;

public class SelectOperation extends BaseOperation {
    FromClause fromClause;
    WhereClause whereClause = null;
    LimitClause limitClause = null;

    public SelectOperation(Operations type) {
        super(type);
    }

    public FromClause getFromClause() {
        return fromClause;
    }
    public WhereClause getWhereClause() {
        return whereClause;
    }
    public LimitClause getLimitClause() {
        return limitClause;
    }

    public void setFromClause(FromClause fromClause) {
        this.fromClause = fromClause;
    }
    public void setWhereClause(WhereClause whereClause) {
        this.whereClause = whereClause;
    }
    public void setLimitClause(LimitClause limitClause) {
        this.limitClause = limitClause;
    }
}
