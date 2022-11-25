package queryParserLayer.clauses;


import java.util.ArrayList;

public class WhereClause extends BaseClause{
    ArrayList<Operation> operations;
    WhereClause(Clauses type, ArrayList<Operation> operations) {
        super(type);
        this.operations = operations;
    }
}
