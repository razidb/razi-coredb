package queryParserLayer.clauses;

public enum Operations {
    // Binary Operands
    EQUAL("="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">="),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    // other
    IN("IN"),
    LIKE("LIKE"),
    IS_NULL("IS NULL");

    String symbol;

    Operations(String symbol){
        this.symbol = symbol;
    }
}
