package queryParserLayer;

import queryParserLayer.operations.BaseOperation;
import queryParserLayer.operations.SelectOperation;

public class ParserResult {
    BaseOperation baseOperation;
    public ParserResult(BaseOperation baseOperation){
        this.baseOperation = baseOperation;
    }

    public String getResourceStr(){
        String resource = "";
        if (baseOperation instanceof SelectOperation){
            resource = ((SelectOperation) baseOperation).getFromClause().getFrom();
        }
        return resource;
    }

    public BaseOperation getBaseOperation() {
        return baseOperation;
    }
}
