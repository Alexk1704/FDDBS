package fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.literal.Literal;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.statement.Statement;
import fdbs.sql.parser.ast.statement.select.SelectNoGroupStatement;
import fdbs.util.DatabaseCursorChecker;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;

public final class NoGroupWithNonJoinHorizontalResultSet extends FedResultSet {

    private SelectNoGroupStatement selectStatement;

    private int currentDb = 0;

    private ResultSetMetaData rs1Metdata = null;
    private ResultSet currentLocalHorizontalResultSet = null;
    private HashMap<Integer, ResultSet> localHorizontalResultSets;

    public NoGroupWithNonJoinHorizontalResultSet(FedConnection fedConnection, Statement statement)
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
        boolean next = false;

        if (currentDb > 0 && currentLocalHorizontalResultSet != null) {
            next = currentLocalHorizontalResultSet.next();
        }

        if (next != true) {
            currentLocalHorizontalResultSet = null;
            while (next == false && Collections.max(localHorizontalResultSets.keySet()) >= currentDb) {
                currentDb++;
                ResultSet localResultSet = localHorizontalResultSets.get(currentDb);
                if (localResultSet != null && localResultSet.next()) {
                    currentLocalHorizontalResultSet = localResultSet;
                    next = true;
                }
            }
        }

        return next;
    }

    @Override
    protected String getPrivateString(int columnIndex) throws Exception {
        return currentLocalHorizontalResultSet.getString(columnIndex);
    }

    @Override
    protected int getPrivateInt(int columnIndex) throws Exception {
        return currentLocalHorizontalResultSet.getInt(columnIndex);
    }

    @Override
    protected int getPrivateColumnCount() throws Exception {
        return rs1Metdata.getColumnCount();
    }

    @Override
    protected String getPrivateColumnName(int columnIndex) throws Exception {
        return rs1Metdata.getColumnName(columnIndex);
    }

    @Override
    protected int getPrivateColumnType(int columnIndex) throws Exception {
        return rs1Metdata.getColumnType(columnIndex);
    }

    @Override
    protected FedException closeLocalResultSets() {
        FedException fedException = null;
        List<SQLException> occuredExceptions = new ArrayList<>();

        if (localHorizontalResultSets != null) {
            for (ResultSet rs : localHorizontalResultSets.values()) {
                try {
                    if (!rs.isClosed()) {
                        try (java.sql.Statement resultSetStatement = rs.getStatement()) {
                            rs.close();
                            DatabaseCursorChecker.closeResultSet(this.getClass());
                        } finally {
                            DatabaseCursorChecker.closeStatement(this.getClass());
                        }
                    }
                } catch (SQLException ex) {
                    occuredExceptions.add(ex);
                }
            }
        }

        rs1Metdata = null;
        currentDb = 0;
        currentLocalHorizontalResultSet = null;

        if (occuredExceptions.size() > 0) {
            fedException = new FedException(String.format("%s: An error occurred while closing local result sets.%n%s",
                    this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));
            for (Exception exception : occuredExceptions) {
                fedException.addSuppressed(exception);
            }
        }

        return fedException;
    }

    @Override
    protected void resolveStatement() throws FedException {
        String tableName = selectStatement.getTableIdentifiers().get(0).getIdentifier();
        Table table = metadataManager.getTable(tableName);
        String queryStatement;
        if (hasFullyQualifiedAttributes()) {
            List<FullyQualifiedAttributeIdentifier> fqAttributes = selectStatement.getFQAttributeIdentifiers();
            String fqAttributesString = concatenateFQAttributes(fqAttributes);
            queryStatement = String.format("SELECT %s FROM %s WHERE %s", fqAttributesString, tableName, getFirstBinaryExpression());
        } else {
            queryStatement = String.format("SELECT * FROM %s WHERE %s", tableName, getFirstBinaryExpression());
        }
        localHorizontalResultSets = getLocalResultSetsHorizontal(table, queryStatement);
        rs1Metdata = initialiseMetaData(localHorizontalResultSets.get(1));
    }

    private boolean hasFullyQualifiedAttributes() {
        return selectStatement.getFQAttributeIdentifiers() != null && !selectStatement.getFQAttributeIdentifiers().isEmpty();
    }

    private String getFirstBinaryExpression() throws FedException {
        String strBinaryExpression;
        BinaryExpression binaryExpression = selectStatement.getWhereClause().getBinaryExpression();

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
}
