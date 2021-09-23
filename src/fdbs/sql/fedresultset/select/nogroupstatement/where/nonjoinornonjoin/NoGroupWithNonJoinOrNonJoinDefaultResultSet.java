package fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinornonjoin;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.literal.*;
import fdbs.sql.parser.ast.statement.Statement;
import fdbs.sql.parser.ast.statement.select.SelectNoGroupStatement;
import fdbs.util.DatabaseCursorChecker;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public final class NoGroupWithNonJoinOrNonJoinDefaultResultSet extends FedResultSet {

    private SelectNoGroupStatement selectStatement;

    private ResultSet localResultSet;
    private ResultSetMetaData rsMetdata = null;

    public NoGroupWithNonJoinOrNonJoinDefaultResultSet(FedConnection fedConnection, Statement statement)
            throws FedException {
        super(fedConnection, statement);
        try {
            selectStatement = (SelectNoGroupStatement) statement;
            resolveStatement();
        } catch (FedException ex) {
            closeLocalResultSets();
            throw ex;
        }
    }

    @Override
    protected boolean privateNext() throws Exception {
        return localResultSet.next();
    }

    @Override
    protected String getPrivateString(int columnIndex) throws Exception {
        return localResultSet.getString(columnIndex);
    }

    @Override
    protected int getPrivateInt(int columnIndex) throws Exception {
        return localResultSet.getInt(columnIndex);
    }

    @Override
    protected int getPrivateColumnCount() throws Exception {
        return rsMetdata.getColumnCount();
    }

    @Override
    protected String getPrivateColumnName(int columnIndex) throws Exception {
        return rsMetdata.getColumnName(columnIndex);
    }

    @Override
    protected int getPrivateColumnType(int columnIndex) throws Exception {
        return rsMetdata.getColumnType(columnIndex);
    }

    @Override
    protected FedException closeLocalResultSets() {
        FedException exception = null;
        if (localResultSet != null) {
            try {
                if (!localResultSet.isClosed()) {
                    try (java.sql.Statement resultSetStatement = localResultSet.getStatement()) {
                        localResultSet.close();
                        DatabaseCursorChecker.closeResultSet(this.getClass());
                    } finally {
                        DatabaseCursorChecker.closeStatement(this.getClass());
                    }
                }
            } catch (SQLException ex) {
                exception = new FedException(String.format("%s: An error occurred while closing local result sets.%n%s",
                        this.getClass().getSimpleName(), ex.getMessage()), ex);
            }
        }

        rsMetdata = null;

        return exception;
    }

    @Override
    protected void resolveStatement() throws FedException {
        String tableName = selectStatement.getTableIdentifiers().get(0).getIdentifier();
        String queryStatement;
        if (hasFullyQualifiedAttributes()) {
            List<FullyQualifiedAttributeIdentifier> fqAttributes = selectStatement.getFQAttributeIdentifiers();
            String fqAttributesString = concatenateFQAttributes(fqAttributes);
            queryStatement = String.format("SELECT %s FROM %s WHERE %s OR %s",
                    fqAttributesString, tableName, getFirstBinaryExpression(), getSecondBinaryExpression());
        } else {
            queryStatement = String.format("SELECT * FROM %s WHERE %s OR %s",
                    tableName, getFirstBinaryExpression(), getSecondBinaryExpression());
        }
        localResultSet = getLocalResultSet(queryStatement, 1);
        rsMetdata = initialiseMetaData(localResultSet);
    }

    private boolean hasFullyQualifiedAttributes() {
        return selectStatement.getFQAttributeIdentifiers() != null && !selectStatement.getFQAttributeIdentifiers().isEmpty();
    }

    private String getFirstBinaryExpression() throws FedException {
        String strBinaryExpression;
        BinaryExpression binaryExpression = (BinaryExpression) selectStatement.getWhereClause().getBinaryExpression()
                .getLeftOperand();
        FullyQualifiedAttributeIdentifier leftOperandFQ = (FullyQualifiedAttributeIdentifier) binaryExpression
                .getLeftOperand();
        String leftOperand = leftOperandFQ.getFullyQualifiedIdentifier();
        String operator = binaryExpression.getSqlOperatorName();
        Literal rightLiteral = (Literal) binaryExpression.getRightOperand();
        String rightOperand = rightLiteral.getIdentifier();

        if (rightLiteral instanceof IntegerLiteral || rightLiteral instanceof NullLiteral) {
            strBinaryExpression = String.format("(%s %s %s)", leftOperand, operator, rightOperand);
        } else {
            strBinaryExpression = String.format("(%s %s '%s')", leftOperand, operator, rightOperand);
        }
        return strBinaryExpression;
    }

    private String getSecondBinaryExpression() throws FedException {
        String strBinaryExpression;
        BinaryExpression binaryExpression = (BinaryExpression) selectStatement.getWhereClause().getBinaryExpression()
                .getRightOperand();
        FullyQualifiedAttributeIdentifier leftOperandFQ
                = (FullyQualifiedAttributeIdentifier) binaryExpression.getLeftOperand();
        String leftOperand = leftOperandFQ.getFullyQualifiedIdentifier();
        String operator = binaryExpression.getSqlOperatorName();
        Literal rightLiteral = (Literal) binaryExpression.getRightOperand();
        String rightOperand = rightLiteral.getIdentifier();

        if (rightLiteral instanceof IntegerLiteral || rightLiteral instanceof NullLiteral) {
            strBinaryExpression = String.format("(%s %s %s)", leftOperand, operator, rightOperand);
        } else {
            strBinaryExpression = String.format("(%s %s '%s')", leftOperand, operator, rightOperand);
        }
        return strBinaryExpression;
    }

}
