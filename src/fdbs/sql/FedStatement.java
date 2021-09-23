package fdbs.sql;

import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.fedresultset.select.countalltablestatement.*;
import fdbs.sql.fedresultset.select.groupbystatement.*;
import fdbs.sql.fedresultset.select.nogroupstatement.*;
import fdbs.sql.fedresultset.select.nogroupstatement.where.join.*;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin.*;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinandnonjoin.*;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinornonjoin.*;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.meta.PartitionType;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.*;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.statement.select.*;
import fdbs.sql.resolver.*;
import fdbs.sql.semantic.SemanticValidator;
import fdbs.sql.unifier.ExecuteUpdateUnifierManager;
import fdbs.util.logger.Logger;
import java.util.List;

public class FedStatement implements FedStatementInterface, AutoCloseable {

    private FedConnection fedConnection;
    protected MetadataManager metadataManager;
    private boolean isClosed = false;
    private FedResultSet lastResultSet = null;

    public FedStatement(FedConnection fedConnection) throws FedException {
        this.fedConnection = fedConnection;
        this.metadataManager = MetadataManager.getInstance(fedConnection);
    }

    @Override
    public int executeUpdate(String statement) throws FedException {
        Logger.infoln(String.format("%s: Executing update statement: %s", this.getClass().getSimpleName(), statement.replaceAll("\\r\\n|\\r|\\n", " ")));
        checkIsClosed();

        int result = 0;

        FedException clearResultSet = clearResultSet();
        if (clearResultSet != null) {
            throw new FedException(String.format("%s: An error occurred while closing last result set.", this.getClass().getSimpleName()), clearResultSet);
        }

        SqlParser sqlParser = new SqlParser(statement);
        try {
            AST ast = sqlParser.parseStatement();
            ast.getRoot().accept(new SemanticValidator(fedConnection));

            StatementResolverManager statementResolver = new StatementResolverManager(fedConnection);
            try (SQLExecuterTask executerTask = statementResolver.resolveStatement(ast)) {
                SQLExecuter sqlExecuter = new SQLExecuter(fedConnection);
                sqlExecuter.executeTask(executerTask);

                ExecuteUpdateUnifierManager statementUnifierManager = new ExecuteUpdateUnifierManager(fedConnection);
                result = statementUnifierManager.unifyUpdateResult(ast, executerTask);
            }
        } catch (Exception ex) {
            throw new FedException(String.format("%s: An error occurred while executing update statement.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        Logger.infoln(String.format("%s: Executed update statement: %s", this.getClass().getSimpleName(), statement.replaceAll("\\r\\n|\\r|\\n", " ")));

        return result;
    }

    @Override
    public FedResultSet executeQuery(String statement) throws FedException {
        Logger.infoln(String.format("%s: Executing query statement: %s.", this.getClass().getSimpleName(), statement.replaceAll("\\r\\n|\\r|\\n", " ")));
        checkIsClosed();

        FedResultSet result = null;

        FedException clearResultSet = clearResultSet();
        if (clearResultSet != null) {
            throw new FedException(String.format("%s: An error occurred while closing last result set.%n%s", this.getClass().getSimpleName(), clearResultSet.getMessage()), clearResultSet);
        }

        SqlParser sqlParser = new SqlParser(statement);
        try {
            AST ast = sqlParser.parseStatement();
            ast.getRoot().accept(new SemanticValidator(fedConnection));
            result = getFedResultSet(ast);
        } catch (Exception ex) {
            throw new FedException(String.format("%s: An error occurred while executing query statement.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        if (result == null) {
            throw new FedException(String.format("%s: An fatal error occurred while trying to resolve right result type class.", this.getClass().getSimpleName()));
        }

        Logger.infoln(String.format("%s: Executed query statement: %s", this.getClass().getSimpleName(), statement.replaceAll("\\r\\n|\\r|\\n", " ")));
        lastResultSet = result;
        return result;
    }

    @Override
    public FedConnection getConnection() throws FedException {
        return fedConnection;
    }

    @Override
    public void close() throws FedException {
        Logger.infoln(String.format("%s: Closing federated statement.", this.getClass().getSimpleName()));

        checkIsClosed();
        isClosed = true;
        FedException clearResultSet = clearResultSet();
        if (clearResultSet != null) {
            throw new FedException(String.format("%s: An error occurred while closing last result set.%n%s", this.getClass().getSimpleName(), clearResultSet.getMessage()), clearResultSet);
        }

        Logger.infoln(String.format("%s: Closed federated statement.", this.getClass().getSimpleName()));
    }

    public boolean isClosed() {
        return isClosed;
    }

    public FedResultSet getResultSet() throws FedException {
        checkIsClosed();
        return lastResultSet;
    }

    private FedException clearResultSet() {
        FedException exception = null;
        if (lastResultSet != null) {
            try {
                if (!lastResultSet.isClosed) {
                    lastResultSet.close();
                }
            } catch (FedException ex) {
                exception = ex;
            }
        }

        return exception;
    }

    private void checkIsClosed() throws FedException {
        if (isClosed) {
            throw new FedException(String.format("%s: The federated statement is closed.", this.getClass().getSimpleName()));
        }
    }

    private FedResultSet getFedResultSet(AST ast) throws FedException {
        FedResultSet resultSet = null;

        ASTNode root = ast.getRoot();

        if (root instanceof SelectStatement) {
            SelectStatement selectStatement = (SelectStatement) ast.getRoot();
            if (selectStatement instanceof SelectCountAllTableStatement) {
                resultSet = getSelectCountAllTableStatementFedResultSet((SelectCountAllTableStatement) selectStatement);
            } else if (selectStatement instanceof SelectNoGroupStatement) {
                resultSet = getSelectNoGroupStatementFedResultSet((SelectNoGroupStatement) selectStatement);
            } else if (selectStatement instanceof SelectGroupStatement) {
                resultSet = new GroupByResultSet(fedConnection, (SelectGroupStatement) selectStatement);
            } else {
                throw new FedException(String.format("%s: The federated select statement type is unknown.", this.getClass().getSimpleName()));
            }
        } else {
            throw new FedException(String.format("%s: The federated execute query statement type is unknown.", this.getClass().getSimpleName()));
        }

        return resultSet;
    }

    private FedResultSet getSelectCountAllTableStatementFedResultSet(SelectCountAllTableStatement statement) throws FedException {
        FedResultSet resultSet = null;

        String tableName = statement.getTableIdentifier().getIdentifier();
        PartitionType partitionType = metadataManager.getPartitionType(tableName);

        switch (partitionType) {
            case None:
                resultSet = new SelectCountAllTableStatementDefaultResultSet(fedConnection, statement);
                break;
            case Vertical:
                resultSet = new SelectCountAllTableStatementVerticalResultSet(fedConnection, statement);
                break;
            case Horizontal:
                resultSet = new SelectCountAllTableStatementHorizontalResultSet(fedConnection, statement);
                break;
            default:
                throw new FedException(String.format("%s: The table \"%s\" partition type \"%s\" is unknwon.", this.getClass().getSimpleName(), tableName, partitionType.name()));
        }

        return resultSet;
    }

    private FedResultSet getSelectNoGroupStatementFedResultSet(SelectNoGroupStatement statement) throws FedException {
        FedResultSet resultSet;
        String tableName1 = statement.getTableIdentifiers().get(0).getIdentifier();
        PartitionType table1PartitionType = metadataManager.getPartitionType(tableName1);

        if (statement.getWhereClause() == null) {
            resultSet = getSelectNoGroupWithoutWhereStatementResultSet(tableName1, table1PartitionType, statement);
        } else {
            resultSet = getSelectNoGroupWithWhereStatementResultSet(tableName1, table1PartitionType, statement);
        }

        return resultSet;
    }

    private FedResultSet getSelectNoGroupWithoutWhereStatementResultSet(String tableName1, PartitionType table1PartitionType, SelectNoGroupStatement statement) throws FedException {
        FedResultSet resultSet = null;

        switch (table1PartitionType) {
            case None:
                resultSet = new NoGroupDefaultResultSet(fedConnection, statement);
                break;
            case Vertical:
                resultSet = new NoGroupVerticalResultSet(fedConnection, statement);
                break;
            case Horizontal:
                resultSet = new NoGroupHorizontalResultSet(fedConnection, statement);
                break;
            default:
                throw new FedException(String.format("%s: The table \"%s\" partition type \"%s\" is unknwon.", this.getClass().getSimpleName(), tableName1, table1PartitionType.name()));
        }
        return resultSet;
    }

    private FedResultSet getSelectNoGroupWithWhereStatementResultSet(String tableName1, PartitionType table1PartitionType, SelectNoGroupStatement statement) throws FedException {
        FedResultSet resultSet = null;
        BinaryExpression binaryExpression = statement.getWhereClause().getBinaryExpression();
        if (binaryExpression.isJoinCondition()) {
            resultSet = new NoGroupWithJoinResultSet(fedConnection, statement);
        } else if (binaryExpression.isNoNJoinCondition()) {
            resultSet = getSelectGroupWithNonJoinStatementResultSet(tableName1, table1PartitionType, statement);
        } else if (binaryExpression.isANestedBinaryExpression()) {
            resultSet = getSelectGroupWithNestedNonJoinWhereStatementResultSet(tableName1, table1PartitionType, binaryExpression, statement);
        }

        return resultSet;
    }

    private FedResultSet getSelectGroupWithNonJoinStatementResultSet(String tableName1, PartitionType table1PartitionType, SelectNoGroupStatement statement) throws FedException {
        FedResultSet resultSet = null;

        switch (table1PartitionType) {
            case None:
                resultSet = new NoGroupWithNonJoinDefaultResultSet(fedConnection, statement);
                break;
            case Vertical:
                resultSet = new NoGroupWithNonJoinVerticalResultSet(fedConnection, statement);
                break;
            case Horizontal:
                resultSet = new NoGroupWithNonJoinHorizontalResultSet(fedConnection, statement);
                break;
            default:
                throw new FedException(String.format("%s: The table \"%s\" partition type \"%s\" is unknwon.", this.getClass().getSimpleName(), tableName1, table1PartitionType.name()));
        }

        return resultSet;
    }

    private FedResultSet getSelectGroupWithNestedNonJoinWhereStatementResultSet(String tableName1, PartitionType table1PartitionType, BinaryExpression nestedBinaryExpression, SelectNoGroupStatement statement) throws FedException {
        FedResultSet resultSet = null;

        Table table1 = metadataManager.getTable(tableName1);

        BinaryExpression leftBinaryExpression = (BinaryExpression) nestedBinaryExpression.getLeftOperand();

        if (leftBinaryExpression.isJoinCondition()) {
            resultSet = new NoGroupWithJoinResultSet(fedConnection, statement);
        } else {
            FullyQualifiedAttributeIdentifier leftBinaryExpressionLeftAttribute = (FullyQualifiedAttributeIdentifier) ((BinaryExpression) nestedBinaryExpression.getLeftOperand()).getLeftOperand();
            FullyQualifiedAttributeIdentifier rightBinaryExpressionLeftAttribute = (FullyQualifiedAttributeIdentifier) ((BinaryExpression) nestedBinaryExpression.getRightOperand()).getLeftOperand();

            if (nestedBinaryExpression.getOperatorName().equals(BinaryExpression.Operator.AND.name())) {
                resultSet = getSelectGroupWithNestedAndNonJoinWhereStatementResultSet(table1, tableName1, table1PartitionType, statement, leftBinaryExpressionLeftAttribute, rightBinaryExpressionLeftAttribute);
            } else if (nestedBinaryExpression.getOperatorName().equals(BinaryExpression.Operator.OR.name())) {
                resultSet = getSelectGroupWithNestedOrNonJoinWhereStatementResultSet(table1, tableName1, table1PartitionType, statement, leftBinaryExpressionLeftAttribute, rightBinaryExpressionLeftAttribute);
            }
        }

        return resultSet;
    }

    private FedResultSet getSelectGroupWithNestedAndNonJoinWhereStatementResultSet(Table table1, String tableName1, PartitionType table1PartitionType, SelectNoGroupStatement statement, FullyQualifiedAttributeIdentifier leftBinaryExpressionLeftAttribute, FullyQualifiedAttributeIdentifier rightBinaryExpressionLeftAttribute) throws FedException {
        FedResultSet resultSet = null;
        int sameDB = getAttributesSameDB(table1, leftBinaryExpressionLeftAttribute.getAttributeIdentifier().getIdentifier(),
                rightBinaryExpressionLeftAttribute.getAttributeIdentifier().getIdentifier());

        switch (table1PartitionType) {
            case None:
                resultSet = new NoGroupWithNonJoinAndNonJoinDefaultResultSet(fedConnection, statement);
                break;
            case Vertical:
                resultSet = new NoGroupWithNonJoinAndNonJoinVerticalResultSet(fedConnection, statement, sameDB);
                break;
            case Horizontal:
                resultSet = new NoGroupWithNonJoinAndNonJoinHorizontalResultSet(fedConnection, statement);
                break;
            default:
                throw new FedException(String.format("%s: The table \"%s\" partition type \"%s\" is unknwon.", this.getClass().getSimpleName(), tableName1, table1PartitionType.name()));
        }

        return resultSet;
    }

    private FedResultSet getSelectGroupWithNestedOrNonJoinWhereStatementResultSet(Table table1, String tableName1, PartitionType table1PartitionType, SelectNoGroupStatement statement, FullyQualifiedAttributeIdentifier leftBinaryExpressionLeftAttribute, FullyQualifiedAttributeIdentifier rightBinaryExpressionLeftAttribute) throws FedException {
        FedResultSet resultSet = null;

        switch (table1PartitionType) {
            case None:
                resultSet = new NoGroupWithNonJoinOrNonJoinDefaultResultSet(fedConnection, statement);
                break;
            case Vertical:
                resultSet = new NoGroupWithNonJoinOrNonJoinVerticalResultSet(fedConnection, statement);
                break;
            case Horizontal:
                resultSet = new NoGroupWithNonJoinOrNonJoinHorizontalResultSet(fedConnection, statement);
                break;
            default:
                throw new FedException(String.format("%s: The table \"%s\" partition type \"%s\" is unknwon.", this.getClass().getSimpleName(), tableName1, table1PartitionType.name()));
        }

        return resultSet;
    }

    private int getAttributesSameDB(Table table, String attributeName1, String attributeName2) {
        int sameDB = 0;

        List<Integer> databasesForColumn1 = table.getDatabasesForColumn(attributeName1);
        List<Integer> databasesForColumn2 = table.getDatabasesForColumn(attributeName2);

        for (int dbId : databasesForColumn1) {
            if (databasesForColumn2.contains(dbId)) {
                sameDB = dbId;
            }
        }

        return sameDB;
    }
}
