package fdbs.sql.semantic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.ast.clause.WhereClause;
import fdbs.sql.parser.ast.function.CountFunction;
import fdbs.sql.parser.ast.function.Function;
import fdbs.sql.parser.ast.function.MaxFunction;
import fdbs.sql.parser.ast.function.SumFunction;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.identifier.TableIdentifier;
import fdbs.sql.parser.ast.statement.select.*;
import fdbs.util.logger.Logger;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * A select statement semantic validator which makes inter alia type checks and
 * integrity checks.
 */
public class SelectStatementSemanticValidator extends SemanticValidator {

    private HashMap<String, Table> tableInfos = new HashMap<>();

    public SelectStatementSemanticValidator(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public void willVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Start select statement semantic validation.", this.getClass().getSimpleName()));
    }

    @Override
    public boolean visit(ASTNode node) throws SemanticValidationException {
        boolean visitChilds = true;

        if (node instanceof SelectCountAllTableStatement) {
            processSelectCountAllTableStatement(node);
        } else if (node instanceof SelectNoGroupStatement) {
            processSelectNoGroupStatement(node);
        } else if (node instanceof SelectGroupStatement) {
            processSelectGroupStatement(node);
        }

        visitChilds = false;
        return visitChilds;
    }

    @Override
    public void didVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Finished select statement semantic validation.", this.getClass().getSimpleName()));
    }

    // <editor-fold defaultstate="collapsed" desc="Private Helper Functions">
    private void processSelectCountAllTableStatement(ASTNode node) throws SemanticValidationException {
        SelectCountAllTableStatement selectStatement = (SelectCountAllTableStatement) node;
        String tableName = selectStatement.getTableIdentifier().getIdentifier();
        Table table = processTableName(tableName, true);
        tableInfos.put(tableName, table);
    }

    private void processSelectGroupStatement(ASTNode node) throws SemanticValidationException {
        SelectGroupStatement selectStatement = (SelectGroupStatement) node;

        for (TableIdentifier tableIdentifier : selectStatement.getTableIdentifiers()) {
            String tableName = tableIdentifier.getIdentifier();
            Table table = processTableName(tableName, true);
            tableInfos.put(tableName, table);
        }

        checkFullQualifiedAttributes(tableInfos, new ArrayList<FullyQualifiedAttributeIdentifier>() {
            {
                add(selectStatement.getFQAttributeIdentifier());
            }
        }, "");

        checkFunction(selectStatement.getFunction());

        WhereClause whereClause = selectStatement.getWhereClause();
        if (whereClause != null) {
            checkWhereClause(tableInfos, whereClause);
        }

        checkFullQualifiedAttributes(tableInfos, new ArrayList<FullyQualifiedAttributeIdentifier>() {
            {
                add(selectStatement.getGroupByClause().getFQAttributeIdentifier());
            }
        }, "Group By - ");

        // Having Clause does not require any check because just Count(*) can be compared with a integer and that is always valid.
    }

    private void processSelectNoGroupStatement(ASTNode node) throws SemanticValidationException {
        SelectNoGroupStatement selectStatement = (SelectNoGroupStatement) node;

        for (TableIdentifier tableIdentifier : selectStatement.getTableIdentifiers()) {
            String tableName = tableIdentifier.getIdentifier();
            Table table = processTableName(tableName, true);
            tableInfos.put(tableName, table);
        }

        checkFullQualifiedAttributes(tableInfos, selectStatement.getFQAttributeIdentifiers(), "");

        WhereClause whereClause = selectStatement.getWhereClause();
        if (whereClause != null) {
            checkWhereClause(tableInfos, whereClause);
        }
    }

    private void checkFunction(Function function) throws SemanticValidationException {
        if (function != null) {
            FullyQualifiedAttributeIdentifier fQAttribute = null;

            if (function instanceof CountFunction) {
                // no check required because attribute can only wildcard *
            } else if (function instanceof MaxFunction) {
                fQAttribute = ((MaxFunction) function).getFQAttributeIdentifier();
            } else if (function instanceof SumFunction) {
                fQAttribute = ((SumFunction) function).getFQAttributeIdentifier();
            } else {
                throw new SemanticValidationException(String.format("%s: The function type \"%s\" is unknown.", this.getClass().getSimpleName(), function.getClass().getName()));
            }

            if (fQAttribute != null) {
                checkSingleFullQualifiedAttribute(tableInfos, fQAttribute, String.format("%s Function - ", function.getIdentifier()));
            }
        }
    }
    // </editor-fold>
}
