package queryParserLayer.clauses;


public class BinaryOperation extends Operation{
    public final String operand1;
    public final Comparable<Object> operand2;

    public BinaryOperation(Operations type, Boolean not, String operand1, Comparable<Object> operand2) {
        super(type, not);
        this.operand1 = operand1;
        this.operand2 = operand2;
    }

    @Override
    public String toString() {
        return operand1 + type.symbol + operand2;
    }

    public boolean apply(Comparable<Object> val) {
        switch (type){
            case EQUAL:
                return !not && operand2.equals(val) || not && !operand2.equals(val);
            case LESS_THAN:
                return val != null && val.compareTo(operand2) < 0;
            case LESS_THAN_OR_EQUAL:
                return val != null && val.compareTo(operand2) <= 0;
            case GREATER_THAN:
                return val != null && val.compareTo(operand2) > 0;
            case GREATER_THAN_OR_EQUAL:
                return val != null && val.compareTo(operand2) >= 0;
            case IN:
            case LIKE:
            case IS_NULL:
                return val == null;
        }

        return false;
    }
}

/*
*
* switch (operation.type) {
                            case EQUAL:
                                if (!operation.not && value == lookup_val){
                                    result.add(document);
                                }
                                else if (operation.not && value != lookup_val){
                                    result.add(document);
                                }

                            case GREATER_THAN:
                                if (value instanceof Comparable<?> && lookup_val != null) {
                                    if (!operation.not && ((Comparable<Object>) value).compareTo(lookup_val) > 0)
                                        result.add(document);
                                    else if (operation.not && ((Comparable<Object>) value).compareTo(lookup_val) != 0)
                                        result.add(document);
                                }

                            case GREATER_THAN_OR_EQUAL:
                                if (value instanceof Comparable<?> && lookup_val != null) {
                                    if (!operation.not && ((Comparable<Object>) value).compareTo(lookup_val) >= 0)
                                        result.add(document);
                                    else if (operation.not && ((Comparable<Object>) value).compareTo(lookup_val) != 0)
                                        result.add(document);
                                }
                            case LESS_THAN:
                                if (value instanceof Comparable<?> && lookup_val != null) {
                                    if (((Comparable<Object>) value).compareTo(lookup_val) < 0)
                                        result.add(document);
                                    else if (((Comparable<Object>) value).compareTo(lookup_val) < 0)
                                        result.add(document);
                                }

                            case LESS_THAN_OR_EQUAL:
                                if (value instanceof Comparable<?> && lookup_val != null) {
                                    if (((Comparable<Object>) value).compareTo(lookup_val) <= 0)
                                        result.add(document);
                                }
                        }
* */