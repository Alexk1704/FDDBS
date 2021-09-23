package fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.fedresultset.ColumnAccessInfo;
import fdbs.sql.fedresultset.MetaDataAccessInfo;
import fdbs.sql.fedresultset.ResultSetAccessInfo;
import fdbs.sql.fedresultset.ResultSetHelpInfo;
import fdbs.sql.meta.Column;
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

public final class NoGroupWithNonJoinVerticalResultSet extends FedResultSet {

    private SelectNoGroupStatement selectStatement;

    private Table table = null;
    private String primaryKeyColumnName = null;
    private int conditionAttributeDb = 0;
    private ArrayList<String> rowAttributes = new ArrayList<>();
    private HashMap<Integer, ResultSetMetaData> metaDatas = null;
    private HashMap<Integer, ColumnAccessInfo> indexColumnAccessInfoMapping = null;

    private ResultSet localVerticalResultSet;
    private HashMap<Integer, ResultSet> otherResultSets = new HashMap<>();
    private HashMap<Integer, ResultSet> localRowResultSets = new HashMap<>();

    private ResultSetHelpInfo helper;

    private boolean forcedNext = false;
    private boolean forcedNextValue = false;

    public NoGroupWithNonJoinVerticalResultSet(FedConnection fedConnection, Statement statement)
            throws FedException {
        super(fedConnection, statement);
        selectStatement = (SelectNoGroupStatement) statement;
        try {
            resolveStatement();
        } catch (FedException ex) {
            closeLocalResultSets();
            throw ex;
        }
    }

    @Override
    protected boolean privateNext() throws Exception {
        boolean next;

        if (forcedNext) {
            next = forcedNextValue;
            forcedNext = false;
        } else {
            next = getNext();
        }

        return next;
    }

    private boolean getNext() throws Exception {
        boolean next = false;

        if (localVerticalResultSet.next()) {
            localRowResultSets.clear();
            if (!otherResultSets.isEmpty()) {
                closeOtherResultSets();
            }

            otherResultSets = getOtherResultSets(helper, localVerticalResultSet.getString(primaryKeyColumnName), new ArrayList<Integer>() {
                {
                    add(conditionAttributeDb);
                }
            });

            localRowResultSets.put(conditionAttributeDb, localVerticalResultSet);
            otherResultSets.entrySet().forEach((e) -> {
                localRowResultSets.put(e.getKey(), e.getValue());
            });

            if (otherResultSets.isEmpty()) {
                next = true;
            } else {
                for (ResultSet rs : otherResultSets.values()) {
                    if (rs.next()) {
                        next = true;
                    } else if (next == true) { // and rs.next() == false
                        throw new FedException(String.format("%s: Unexpceted local results. Not all local result sets have a result.",
                                this.getClass().getSimpleName()));
                    }
                }
            }

            metaDatas = initialiseMetaDatas(localRowResultSets);

            if (indexColumnAccessInfoMapping == null) {
                indexColumnAccessInfoMapping = getColumnAccessInfo(metaDatas, rowAttributes);
            }
        }

        return next;
    }

    private void closeOtherResultSets() throws FedException {
        List<SQLException> occuredExceptions = new ArrayList<>();

        for (ResultSet rs : otherResultSets.values()) {
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

        if (occuredExceptions.size() > 0) {
            FedException fedException = new FedException(String.format("%s: An error occurred while closing local result sets.%n%s",
                    this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));
            for (Exception exception : occuredExceptions) {
                fedException.addSuppressed(exception);
                throw fedException;
            }

            throw fedException;
        }
    }

    @Override
    protected String getPrivateString(int columnIndex) throws Exception {
        ResultSetAccessInfo resultSetAccessInfo = getResultSetAccessInfo(columnIndex, localRowResultSets, indexColumnAccessInfoMapping);
        return resultSetAccessInfo.getResultSet().getString(resultSetAccessInfo.getColumnIndex());
    }

    @Override
    protected int getPrivateInt(int columnIndex) throws Exception {
        ResultSetAccessInfo resultSetAccessInfo = getResultSetAccessInfo(columnIndex, localRowResultSets, indexColumnAccessInfoMapping);
        return resultSetAccessInfo.getResultSet().getInt(resultSetAccessInfo.getColumnIndex());
    }

    @Override
    protected int getPrivateColumnCount() throws Exception {
        return rowAttributes.size();
    }

    @Override
    protected String getPrivateColumnName(int columnIndex) throws Exception {
        if (metaDatas == null || indexColumnAccessInfoMapping == null) {
            forcedNextValue = getNext();
            forcedNext = true;
            if (!forcedNextValue && (metaDatas == null || indexColumnAccessInfoMapping == null)) {
                forceMetaDataInitialization();
            }
        }

        MetaDataAccessInfo metaDataAccessInfo = getMetaDataAccessInfo(columnIndex, metaDatas, indexColumnAccessInfoMapping);
        return metaDataAccessInfo.getMetaData().getColumnName(metaDataAccessInfo.getColumnIndex());
    }

    @Override
    protected int getPrivateColumnType(int columnIndex) throws Exception {
        if (metaDatas == null || indexColumnAccessInfoMapping == null) {
            forcedNextValue = getNext();
            forcedNext = true;
            if (!forcedNextValue && (metaDatas == null || indexColumnAccessInfoMapping == null)) {
                forceMetaDataInitialization();
            }
        }

        MetaDataAccessInfo metaDataAccessInfo = getMetaDataAccessInfo(columnIndex, metaDatas, indexColumnAccessInfoMapping);
        return metaDataAccessInfo.getMetaData().getColumnType(metaDataAccessInfo.getColumnIndex());
    }

    @Override
    protected FedException closeLocalResultSets() {
        FedException fedException = null;
        List<SQLException> occuredExceptions = new ArrayList<>();

        if (localVerticalResultSet != null) {
            try {
                if (!localVerticalResultSet.isClosed()) {
                    try (java.sql.Statement resultSetStatement = localVerticalResultSet.getStatement()) {
                        localVerticalResultSet.close();
                        DatabaseCursorChecker.closeResultSet(this.getClass());
                    } finally {
                        DatabaseCursorChecker.closeStatement(this.getClass());
                    }
                }
            } catch (SQLException ex) {
                occuredExceptions.add(ex);
            }
        }

        if (localRowResultSets != null) {
            for (ResultSet rs : localRowResultSets.values()) {
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

        table = null;
        primaryKeyColumnName = null;
        conditionAttributeDb = 0;
        rowAttributes.clear();
        metaDatas = null;
        indexColumnAccessInfoMapping = null;

        forcedNext = false;
        forcedNextValue = false;

        if (occuredExceptions.size() > 0) {
            fedException = new FedException(String.format("%s: An error occurred while closing local result sets.%n%s", this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));
            for (Exception exception : occuredExceptions) {
                fedException.addSuppressed(exception);
            }
        }

        return fedException;
    }

    @Override
    protected void resolveStatement() throws FedException {
        String tableName = selectStatement.getTableIdentifiers().get(0).getIdentifier();
        table = metadataManager.getTable(tableName);
        primaryKeyColumnName = table.getPrimaryKeyConstraints().get(0).getAttributeName();

        BinaryExpression binaryExpression = selectStatement.getWhereClause().getBinaryExpression();
        FullyQualifiedAttributeIdentifier leftOperandFQ = (FullyQualifiedAttributeIdentifier) binaryExpression.getLeftOperand();
        conditionAttributeDb = table.getColumn(leftOperandFQ.getAttributeIdentifier().getIdentifier()).getDatabase();

        String queryStatement;
        if (hasFullyQualifiedAttributes()) {
            List<FullyQualifiedAttributeIdentifier> fqAttributes = selectStatement.getFQAttributeIdentifiers();
            initializeFQRowAttributes(fqAttributes);
            HashMap<Integer, String> fqAttributesForEachDB = getFQAttributesForEachDB(table, fqAttributes);

            boolean containsPrimaryKey = checkFullyQualifiedAttributesForPK(fqAttributes);
            if (containsPrimaryKey) {
                queryStatement = String.format("SELECT %s FROM %s WHERE %s",
                        fqAttributesForEachDB.get(conditionAttributeDb), tableName, getFirstBinaryExpression());
            } else {
                String fullyQualifiedPrimaryKey = String.format("%s.%s", tableName, primaryKeyColumnName);
                String conditionAttributesDBAttributeList = fqAttributesForEachDB.get(conditionAttributeDb);
                if (conditionAttributesDBAttributeList.isEmpty()) {
                    queryStatement = String.format("SELECT %s FROM %s WHERE %s", fullyQualifiedPrimaryKey, tableName, getFirstBinaryExpression());
                } else {
                    queryStatement = String.format("SELECT %s,%s FROM %s WHERE %s",
                            fqAttributesForEachDB.get(conditionAttributeDb), fullyQualifiedPrimaryKey, tableName, getFirstBinaryExpression());
                }
            }

            initializeResultSetHelper(tableName, fqAttributesForEachDB);
        } else {
            initializeAllRowAttributes();
            queryStatement = String.format("SELECT * FROM %s WHERE %s", tableName, getFirstBinaryExpression());
            initializeResultSetHelper(tableName, null);
        }

        localVerticalResultSet = getLocalResultSet(queryStatement, conditionAttributeDb);
    }

    private boolean hasFullyQualifiedAttributes() {
        return selectStatement.getFQAttributeIdentifiers() != null && !selectStatement.getFQAttributeIdentifiers().isEmpty();
    }

    private void initializeFQRowAttributes(List<FullyQualifiedAttributeIdentifier> fqAttributes) {
        for (FullyQualifiedAttributeIdentifier attribute : fqAttributes) {
            String attributeName = attribute.getAttributeIdentifier().getIdentifier();
            rowAttributes.add(attributeName);
        }
    }

    private boolean checkFullyQualifiedAttributesForPK(List<FullyQualifiedAttributeIdentifier> fqAttributes) {
        HashMap<Integer, List<FullyQualifiedAttributeIdentifier>> fqAttributeListForEachDb = getFQAttributesForEachDbAsList(table, fqAttributes);
        List<FullyQualifiedAttributeIdentifier> fqaiList = fqAttributeListForEachDb.get(conditionAttributeDb);
        boolean containsPrimaryKey = false;
        for (FullyQualifiedAttributeIdentifier fqai : fqaiList) {
            if (fqai.getAttributeIdentifier().getIdentifier().equals(primaryKeyColumnName)) {
                containsPrimaryKey = true;
                break;
            }
        }
        return containsPrimaryKey;
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

    private void initializeAllRowAttributes() {
        ArrayList<Column> dbColumns = table.getColumns();
        for (Column c : dbColumns) {
            String attribute = c.getAttributeName();
            rowAttributes.add(attribute);
        }
    }

    private void initializeResultSetHelper(String tableName, HashMap<Integer, String> fqAttributesForEachDB) {
        String fromStatement;

        if (table.getColumn(primaryKeyColumnName).getType().toUpperCase().equals("INTEGER")) {
            fromStatement = "FROM %s WHERE (%s = %s)";
        } else {
            fromStatement = "FROM %s WHERE (%s = '%s')";
        }

        ArrayList<String> paramList = new ArrayList();
        paramList.add(table.getAttributeName());
        paramList.add(tableName + "." + primaryKeyColumnName);

        if (fqAttributesForEachDB == null) {
            helper = new ResultSetHelpInfo(table, fromStatement, paramList);
        } else {
            helper = new ResultSetHelpInfo(fqAttributesForEachDB, table, fromStatement, paramList);
        }
    }

    private void forceMetaDataInitialization() throws Exception {
        localRowResultSets.clear();
        if (!otherResultSets.isEmpty()) {
            closeOtherResultSets();
        }
        ResultSetHelpInfo forcedHelper = new ResultSetHelpInfo(helper.getFqAttributesForEachDB(), table, "FROM %s", new ArrayList<String>() {
            {
                add(table.getAttributeName());
            }
        });
        otherResultSets = getOtherResultSets(forcedHelper, null, new ArrayList<Integer>() {
            {
                add(conditionAttributeDb);
            }
        });

        localRowResultSets.put(conditionAttributeDb, localVerticalResultSet);
        otherResultSets.entrySet().forEach((e) -> {
            localRowResultSets.put(e.getKey(), e.getValue());
        });

        metaDatas = initialiseMetaDatas(localRowResultSets);
        indexColumnAccessInfoMapping = getColumnAccessInfo(metaDatas, rowAttributes);
    }
}
