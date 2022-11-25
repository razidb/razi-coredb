package queryParserLayer;

import queryParserLayer.clauses.Clauses;
import queryParserLayer.clauses.FromClause;
import queryParserLayer.clauses.Operation;
import queryParserLayer.operations.BaseOperation;
import queryParserLayer.operations.MainOperations;
import queryParserLayer.operations.SelectOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Parser {
    private static Parser parser = null;

    public static Parser getQueryParser(){
        if (parser == null)
            parser = new Parser();
        return parser;
    }

    public static ParserResult parse(String statement) throws Exception {
        List<String> stParts = new java.util.ArrayList<String>(List.of(statement.split(" ")));
        MainOperations operation = MainOperations.valueOf(stParts.remove(0).toUpperCase());
        BaseOperation operationInstance = null;
        switch (operation){
            case SELECT:
                operationInstance = new SelectOperation(MainOperations.SELECT);
                // get fields
                String fields = stParts.remove(0);
                operationInstance.setArgs(fields.split(","));
                // get from clause
                Clauses fromClause = Clauses.valueOf(stParts.remove(0).toUpperCase());
                FromClause fromClauseInstance = new FromClause();
                if (fromClause != Clauses.FROM)
                    throw new Exception("Invalid syntax");
                // get table name
                String tableName = stParts.remove(0);
                fromClauseInstance.setFrom(tableName);
                // get clauses (where, limit)
                Clauses whereClause = null;
                Clauses limitClause = null;
                Clauses clause = Clauses.valueOf(stParts.remove(0).toUpperCase());
                if (clause == Clauses.WHERE)
                    whereClause = clause;
                else if (clause == Clauses.LIMIT)
                    limitClause = clause;
                else
                    throw new Exception("Invalid syntax");
                if (!Objects.equals(stParts.get(0), ";")){
                    if (whereClause != null){
                        ArrayList<Operation> operations = new ArrayList<Operation>();

                    }
                }
            case UPDATE:
            case DELETE:
            case CREATE:

        }
        if (operationInstance == null)
            throw new Exception("Invalid operation: " + operation);
        return new ParserResult(operationInstance);

    }


}


/*
## Select Statement ##
selectStmt  := gsqlSelectBlock | sqlSelectBlock

gsqlSelectBlock := gsqlSelectClause
               fromClause
               [sampleClause]
               [whereClause]
               [accumClause]
               [postAccumClause]*
               [havingClause]
               [orderClause]
               [limitClause]

sqlSelectBlock := sqlSelectClause
               fromClause
               [whereClause]
               [groupByClause]
               [havingClause]
               [orderClause]
               [limitClause]

gsqlSelectClause := vertexSetName "=" SELECT vertexAlias
sqlSelectClause := SELECT [DISTINCT] columnExpr ("," columnExpr)*
                   INTO tableName
columnExpr := expr [AS columnName]
            | aggregator "("[DISTINCT] expr ")" [AS columnName]
columnName := name
tableName := name

fromClause := FROM (step | stepV2 | pathPattern ["," pathPattern]*)

step   :=  stepSourceSet ["-" "(" stepEdgeSet ")" ("-"|"->") stepVertexSet]
stepV2 :=  stepVertexSet ["-" "(" stepEdgeSet ")" "-" stepVertexSet]

stepSourceSet := vertexSetName [":" vertexAlias]
stepEdgeSet := [stepEdgeTypes] [":" edgeAlias]
stepVertexSet := [stepVertexTypes] [":" vertexAlias]
alias := (vertexAlias | edgeAlias)
vertexAlias := name
edgeAlias := name

stepEdgeTypes := atomicEdgeType | "(" edgeSetType ["|" edgeSetType]* ")"
atomicEdgeType := "_" | ANY | edgeSetType
edgeSetType := edgeType | paramName | globalAccumName

stepVertexTypes := atomicVertexType | "(" vertexSetType ["|" vertexSetType]* ")"
atomicVertexType := "_" | ANY | vertexSetType
vertexSetType := vertexType | paramName | globalAccumName

*/