package fdbs.sql;

import fdbs.sql.fedresultset.ColumnAccessInfo;
import fdbs.sql.fedresultset.MetaDataAccessInfo;
import fdbs.sql.fedresultset.ResultSetAccessInfo;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import fdbs.sql.fedresultset.ResultSetHelpInfo;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.identifier.AttributeIdentifier;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.statement.Statement;
import fdbs.util.DatabaseCursorChecker;
import fdbs.util.logger.Logger;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class FedResultSet implements FedResultSetInterface, AutoCloseable {

    protected MetadataManager metadataManager;
    protected FedConnection fedConnection;
    protected Statement statement;
    protected boolean isClosed = false;
    private boolean didNext = false;
    private boolean lastNextResult = false;

    public FedResultSet(FedConnection fedConnection, Statement statement) throws FedException {
        this.fedConnection = fedConnection;
        this.metadataManager = MetadataManager.getInstance(fedConnection);
        this.statement = statement;
    }

    @Override
    public boolean next() throws FedException {
        checkIsClosed();
        didNext = true;
        boolean next = false;
        try {
            next = privateNext();
        } catch (Exception ex) {
            throw new FedException(String.format("%s: An error occurred while calling FedResultSet.next().%n%s",
                    this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        lastNextResult = !next;
        return next;
    }

    @Override
    public String getString(int columnIndex) throws FedException {
        checkIsClosed();
        checkDidNext();
        checkIsNotLast();

        String value = "";
        try {
            value = getPrivateString(columnIndex);
        } catch (Exception ex) {;
            throw new FedException(String.format("%s: An error occurred while calling FedResultSet.getString().%n%s",
                    this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return value;
    }

    @Override
    public int getInt(int columnIndex) throws FedException {
        checkIsClosed();
        checkDidNext();
        checkIsNotLast();

        int value = 0;
        try {
            value = getPrivateInt(columnIndex);
        } catch (Exception ex) {
            throw new FedException(String.format("%s: An error occurred while calling FedResultSet.getInt().%n%s",
                    this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return value;
    }

    @Override
    public int getColumnCount() throws FedException {
        checkIsClosed();

        int value = 0;
        try {
            value = getPrivateColumnCount();
        } catch (Exception ex) {
            throw new FedException(String.format("%s: An error occurred while calling FedResultSet.getColumnCount().%n%s",
                    this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return value;
    }

    @Override
    public String getColumnName(int columnIndex) throws FedException {
        checkIsClosed();

        String value = "";
        try {
            value = getPrivateColumnName(columnIndex);
        } catch (Exception ex) {
            throw new FedException(String.format("%s: An error occurred while calling FedResultSet.getColumnName().%n%s",
                    this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return value;
    }

    @Override
    public int getColumnType(int columnIndex) throws FedException {
        checkIsClosed();

        int value = 0;
        try {
            value = getPrivateColumnType(columnIndex);
        } catch (Exception ex) {
            throw new FedException(String.format("%s: An error occurred while calling FedResultSet.getColumnType().%n%s",
                    this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return value;
    }

    @Override
    public void close() throws FedException {
        Logger.infoln(String.format("%s: Closing federated statement.", this.getClass().getSimpleName()));

        checkIsClosed();
        isClosed = true;
        FedException clearResultSetException = closeLocalResultSets();
        if (clearResultSetException != null) {
            throw new FedException(String.format("%s: An error occurred while closing federated result set.%n%s",
                    this.getClass().getSimpleName(), clearResultSetException.getMessage()), clearResultSetException);
        }

        Logger.infoln(String.format("%s: Closed federated result set.", this.getClass().getSimpleName()));
    }

    public boolean isClosed() {
        return isClosed;
    }

    private void checkDidNext() throws FedException {
        if (!didNext) {
            throw new FedException(String.format("%s: ResultSet.next was not called.", this.getClass().getName()));
        }
    }

    private void checkIsNotLast() throws FedException {
        if (didNext && lastNextResult) {
            throw new FedException(String.format("%s: Accessed After end of result set.", this.getClass().getName()));
        }
    }

    protected abstract boolean privateNext() throws Exception;

    protected abstract String getPrivateString(int columnIndex) throws Exception;

    protected abstract int getPrivateInt(int columnIndex) throws Exception;

    protected abstract int getPrivateColumnCount() throws Exception;

    protected abstract String getPrivateColumnName(int columnIndex) throws Exception;

    protected abstract int getPrivateColumnType(int columnIndex) throws Exception;

    protected abstract FedException closeLocalResultSets();

    protected abstract void resolveStatement() throws Exception;

    protected ResultSet getLocalResultSet(String stmt, int requestingDB) throws FedException {
        ResultSet resultSet = null;
        Connection dbConnection = fedConnection.getConnections().get(requestingDB);

        try {
            java.sql.Statement selectStatement = dbConnection.createStatement();
            Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), requestingDB, stmt));
            resultSet = selectStatement.executeQuery(stmt);
            DatabaseCursorChecker.openStatement(this.getClass());
            DatabaseCursorChecker.openResultSet(this.getClass());
        } catch (SQLException ex) {
            FedException fedException = new FedException(
                    String.format("%s: An error occurred while retrieving local result set for database \"%s\".%n%s",
                            this.getClass().getSimpleName(), requestingDB, ex.getMessage()));

            java.sql.Statement selectStatement = null;
            try {
                if (resultSet != null) {
                    selectStatement = resultSet.getStatement();
                    resultSet.close();
                    DatabaseCursorChecker.closeResultSet(this.getClass());
                }
            } catch (SQLException ex2) {
                //ignore exception
            }

            try {
                if (selectStatement != null) {
                    selectStatement.close();
                    DatabaseCursorChecker.closeStatement(this.getClass());
                }
            } catch (SQLException ex2) {
                //ignore exception
            }

            throw fedException;
        }

        return resultSet;
    }

    protected HashMap<Integer, ResultSet> getLocalResultSetsHorizontalWithoutDBx(Table table, String stmt, int dbX)
            throws FedException {

        ResultSet resultSetDb1, resultSetDb2, resultSetDb3;

        HashMap<Integer, ResultSet> horizontalResultSets = new HashMap<>();

        HashMap<Integer, LinkedHashMap<String, Column>> databaseRelatedColumns = table.getDatabaseRelatedColumns();

        try {

            if (!(databaseRelatedColumns.get(1).isEmpty()) && dbX != 1) {
                resultSetDb1 = getLocalResultSet(stmt, 1);
                horizontalResultSets.put(1, resultSetDb1);
            }
            if (!(databaseRelatedColumns.get(2).isEmpty()) && dbX != 2) {
                resultSetDb2 = getLocalResultSet(stmt, 2);
                horizontalResultSets.put(2, resultSetDb2);
            }
            if (!(databaseRelatedColumns.get(3).isEmpty()) && dbX != 3) {
                resultSetDb3 = getLocalResultSet(stmt, 3);
                horizontalResultSets.put(3, resultSetDb3);
            }
        } catch (FedException ex) {
            for (ResultSet rs : horizontalResultSets.values()) {
                try {
                    if (!rs.isClosed()) {
                        try (java.sql.Statement resultSetStatement = rs.getStatement()) {
                            rs.close();
                            DatabaseCursorChecker.closeResultSet(this.getClass());
                        } finally {
                            DatabaseCursorChecker.closeStatement(this.getClass());
                        }
                    }
                } catch (SQLException ex2) {
                    //ignore exception
                }
            }
            throw ex;
        }

        return horizontalResultSets;
    }

    protected HashMap<Integer, ResultSet> getLocalResultSetsHorizontal(Table table, String stmt)
            throws FedException {

        ResultSet resultSetDb1, resultSetDb2, resultSetDb3;

        HashMap<Integer, ResultSet> horizontalResultSets = new HashMap<>();

        HashMap<Integer, LinkedHashMap<String, Column>> databaseRelatedColumns = table.getDatabaseRelatedColumns();

        try {
            if (!(databaseRelatedColumns.get(1).isEmpty())) {
                resultSetDb1 = getLocalResultSet(stmt, 1);
                horizontalResultSets.put(1, resultSetDb1);
            }
            if (!(databaseRelatedColumns.get(2).isEmpty())) {
                resultSetDb2 = getLocalResultSet(stmt, 2);
                horizontalResultSets.put(2, resultSetDb2);
            }
            if (!(databaseRelatedColumns.get(3).isEmpty())) {
                resultSetDb3 = getLocalResultSet(stmt, 3);
                horizontalResultSets.put(3, resultSetDb3);
            }
        } catch (FedException ex) {
            for (ResultSet rs : horizontalResultSets.values()) {
                try {
                    if (!rs.isClosed()) {
                        try (java.sql.Statement resultSetStatement = rs.getStatement()) {
                            rs.close();
                            DatabaseCursorChecker.closeResultSet(this.getClass());
                        } finally {
                            DatabaseCursorChecker.closeStatement(this.getClass());
                        }
                    }
                } catch (SQLException ex2) {
                    //ignore exception
                }
            }
            throw ex;
        }

        return horizontalResultSets;
    }

    protected void checkIsClosed() throws FedException {
        if (isClosed) {
            throw new FedException(String.format("%s: The federated result set is closed.", this.getClass().getName()));
        }
    }

    protected int getAmountOfInvolvedDBs(String tableName, Table table) throws FedException {
        HashMap<Integer, LinkedHashMap<String, Column>> databaseRelatedColumns = table.getDatabaseRelatedColumns();
        //Kann nur 2||3 sein.
        int amount = 3;
        if (databaseRelatedColumns.get(3).isEmpty()) {
            amount = 2;
        }
        return amount;
    }

    protected HashMap<Integer, ResultSet> getLocalVerticalResultSets(final String tableName, String stmt)
            throws FedException {
        ResultSet resultSetDB1, resultSetDB2, resultSetDB3;
        HashMap<Integer, ResultSet> localVerticalResultSets = new HashMap<>();
        Table table = metadataManager.getTable(tableName);
        int amountOfInvolvedDBs = getAmountOfInvolvedDBs(tableName, table);
        try {
            if (amountOfInvolvedDBs == 3) {
                resultSetDB1 = getLocalResultSet(stmt, 1);
                localVerticalResultSets.put(1, resultSetDB1);
                resultSetDB2 = getLocalResultSet(stmt, 2);
                localVerticalResultSets.put(2, resultSetDB2);
                resultSetDB3 = getLocalResultSet(stmt, 3);
                localVerticalResultSets.put(3, resultSetDB3);
            } else if (amountOfInvolvedDBs == 2) {
                resultSetDB1 = getLocalResultSet(stmt, 1);
                localVerticalResultSets.put(1, resultSetDB1);
                resultSetDB2 = getLocalResultSet(stmt, 2);
                localVerticalResultSets.put(2, resultSetDB2);
            } else {
                throw new FedException(String.format("%s: Unexpected vertical partitioning.", this.getClass().getName()));
            }
        } catch (FedException ex) {
            for (ResultSet rs : localVerticalResultSets.values()) {
                try {
                    if (!rs.isClosed()) {
                        try (java.sql.Statement resultSetStatement = rs.getStatement()) {
                            rs.close();
                            DatabaseCursorChecker.closeResultSet(this.getClass());
                        } finally {
                            DatabaseCursorChecker.closeStatement(this.getClass());
                        }
                    }
                } catch (SQLException ex2) {
                    //ignore exception
                }
            }
            throw ex;
        }

        return localVerticalResultSets;
    }

    protected String concatenateFQAttributes(List<FullyQualifiedAttributeIdentifier> fqAttributes) {
        String allFQAttributesAsString = "";
        Boolean isFirst = true;
        for (FullyQualifiedAttributeIdentifier fqAtt : fqAttributes) {
            String concatName = "";
            String fQName = fqAtt.getFullyQualifiedIdentifier();
            if (isFirst) {
                isFirst = false;
                concatName = fQName;
            } else {
                concatName = String.format(" ,%s", fQName);
            }
            allFQAttributesAsString = allFQAttributesAsString.concat(concatName);
        }

        return allFQAttributesAsString;
    }

    protected ResultSetMetaData initialiseMetaData(ResultSet resultSet) throws FedException {
        ResultSetMetaData metaData = null;
        if (resultSet != null) {
            try {
                metaData = resultSet.getMetaData();
            } catch (SQLException ex) {
                throw new FedException(String.format("%s: An error occurred while retrieving metadata from result set.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
            }
        }

        return metaData;
    }

    protected HashMap<Integer, ResultSetMetaData> initialiseMetaDatas(HashMap<Integer, ResultSet> resultSets) throws FedException {
        HashMap<Integer, ResultSetMetaData> metaDatas = new HashMap<>();;

        if (resultSets != null) {
            try {
                for (int dbId : resultSets.keySet()) {
                    ResultSet rs = resultSets.get(dbId);
                    metaDatas.put(dbId, rs.getMetaData());
                }
            } catch (SQLException ex) {
                throw new FedException(String.format("%s: An error occurred while retrieving metadata from result set.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
            }
        }

        return metaDatas;
    }

    protected MetaDataAccessInfo getMetaDataAccessInfo(int columnIndex, HashMap<Integer, ResultSetMetaData> metaDatas, HashMap<Integer, ColumnAccessInfo> indexColumAccessInfoMapping) throws FedException {
        if (!indexColumAccessInfoMapping.containsKey(columnIndex)) {
            throw new FedException(String.format("%s: Invalid column index.", this.getClass().getSimpleName()));
        }

        ColumnAccessInfo accessInfo = indexColumAccessInfoMapping.get(columnIndex);

        if (!metaDatas.keySet().contains(accessInfo.getResultSetDbId())) {
            throw new FedException(String.format("%s: Fatal Error - Could not resolver result set metadata.", this.getClass().getSimpleName()));
        }

        ResultSetMetaData metaData = metaDatas.get(accessInfo.getResultSetDbId());
        int metaDataColumnIndex = accessInfo.getResultSetIndex();

        return new MetaDataAccessInfo(metaData, metaDataColumnIndex);
    }

    protected ResultSetAccessInfo getResultSetAccessInfo(int columnIndex, HashMap<Integer, ResultSet> localVerticalResultSets, HashMap<Integer, ColumnAccessInfo> indexColumAccessInfoMapping) throws FedException {
        if (!indexColumAccessInfoMapping.containsKey(columnIndex)) {
            throw new FedException(String.format("%s: Invalid column index.", this.getClass().getSimpleName()));
        }

        ColumnAccessInfo accessInfo = indexColumAccessInfoMapping.get(columnIndex);

        if (!localVerticalResultSets.keySet().contains(accessInfo.getResultSetDbId())) {
            throw new FedException(String.format("%s: Fatal Error - Could not resolver result set metadata.", this.getClass().getSimpleName()));
        }

        ResultSet resultSet = localVerticalResultSets.get(accessInfo.getResultSetDbId());
        int resultSetColumnIndex = accessInfo.getResultSetIndex();

        return new ResultSetAccessInfo(resultSet, resultSetColumnIndex);
    }

    protected HashMap<Integer, ColumnAccessInfo> getColumnAccessInfo(HashMap<Integer, ResultSetMetaData> metaDatas, List<String> selectContainingAttributes) throws FedException {
        HashMap<Integer, ColumnAccessInfo> columnAccessInfo = new HashMap<>();

        for (Map.Entry<Integer, ResultSetMetaData> entry : metaDatas.entrySet()) {
            HashMap<String, ColumnAccessInfo> attributeColumnAccessInfoMapping = new HashMap<>();

            int dbId = entry.getKey();
            ResultSetMetaData metaData = entry.getValue();

            try {
                int columnsCount = metaData.getColumnCount();
                for (int rsIndex = 1; rsIndex <= columnsCount; rsIndex++) {
                    String columnName = metaData.getColumnName(rsIndex);
                    attributeColumnAccessInfoMapping.put(columnName, new ColumnAccessInfo(dbId, rsIndex));
                }
            } catch (SQLException ex) {
                throw new FedException(String.format("%s: An error occurred while retrieving column infos form result set metadata.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
            }

            for (int rowIndex = 1; rowIndex <= selectContainingAttributes.size(); rowIndex++) {
                String attribute = selectContainingAttributes.get(rowIndex - 1);
                if (attributeColumnAccessInfoMapping.containsKey(attribute)) {
                    columnAccessInfo.put(rowIndex, attributeColumnAccessInfoMapping.get(attribute));
                }
            }
        }

        if (columnAccessInfo.size() != selectContainingAttributes.size()) {
            throw new FedException(String.format("%s: An error occurred while retrieving column infos form result set metadata. Found not all required column infos.", this.getClass().getSimpleName()));
        }

        return columnAccessInfo;
    }

    protected HashMap<Integer, String> getFQAttributesForEachDB(Table table,
            List<FullyQualifiedAttributeIdentifier> fqAttributes) {
        HashMap<Integer, String> stringsForEachDB = new HashMap<>();
        List<FullyQualifiedAttributeIdentifier> fqAttributesListForDB1 = new ArrayList<>();
        List<FullyQualifiedAttributeIdentifier> fqAttributesListForDB2 = new ArrayList<>();
        List<FullyQualifiedAttributeIdentifier> fqAttributesListForDB3 = new ArrayList<>();

        for (FullyQualifiedAttributeIdentifier fqai : fqAttributes) {
            AttributeIdentifier identifier = fqai.getAttributeIdentifier();
            String temp = identifier.getIdentifier();
            List<Integer> database = table.getDatabasesForColumn(temp);
            for (Integer i : database) {
                if (i == 1) {
                    fqAttributesListForDB1.add(fqai);
                }
                if (i == 2) {
                    fqAttributesListForDB2.add(fqai);
                }
                if (i == 3) {
                    fqAttributesListForDB3.add(fqai);
                }
            }
        }
        String fqAttributesForDB1 = concatenateFQAttributes(fqAttributesListForDB1);
        String fqAttributesForDB2 = concatenateFQAttributes(fqAttributesListForDB2);
        String fqAttributesForDB3 = concatenateFQAttributes(fqAttributesListForDB3);
        stringsForEachDB.put(1, fqAttributesForDB1);
        stringsForEachDB.put(2, fqAttributesForDB2);
        stringsForEachDB.put(3, fqAttributesForDB3);

        return stringsForEachDB;
    }

    protected HashMap<Integer, List<FullyQualifiedAttributeIdentifier>> getFQAttributesForEachDbAsList(Table table,
            List<FullyQualifiedAttributeIdentifier> fqAttributes) {
        HashMap<Integer, List<FullyQualifiedAttributeIdentifier>> stringsForEachDB = new HashMap<>();
        List<FullyQualifiedAttributeIdentifier> fqAttributesListForDB1 = new ArrayList<>();
        List<FullyQualifiedAttributeIdentifier> fqAttributesListForDB2 = new ArrayList<>();
        List<FullyQualifiedAttributeIdentifier> fqAttributesListForDB3 = new ArrayList<>();

        for (FullyQualifiedAttributeIdentifier fqai : fqAttributes) {
            AttributeIdentifier identifier = fqai.getAttributeIdentifier();
            String temp = identifier.getIdentifier();
            List<Integer> database = table.getDatabasesForColumn(temp);
            for (Integer i : database) {
                if (i == 1) {
                    fqAttributesListForDB1.add(fqai);
                }
                if (i == 2) {
                    fqAttributesListForDB2.add(fqai);
                }
                if (i == 3) {
                    fqAttributesListForDB3.add(fqai);
                }
            }
        }
        stringsForEachDB.put(1, fqAttributesListForDB1);
        stringsForEachDB.put(2, fqAttributesListForDB2);
        stringsForEachDB.put(3, fqAttributesListForDB3);

        return stringsForEachDB;
    }

    protected HashMap<Integer, ResultSet> getResultSetsForVerticalWithoutAllAttribute(
            HashMap<Integer, String> fqaStringsForEachDB, String stmtStartWithFromForDB1,
            String stmtStartWithFromForDB2, String stmtStartWithFromForDB3) throws FedException {
        if (!(stmtStartWithFromForDB1.trim().startsWith("FROM")) || !(stmtStartWithFromForDB2.trim().startsWith("FROM"))
                || !(stmtStartWithFromForDB3.trim().startsWith("FROM"))) {
            throw new FedException(String.format("%s: Fatal Error - String must start with 'FROM'", this.getClass().getSimpleName()));
        }

        HashMap<Integer, ResultSet> resultSets = new HashMap<>();
        try {
            String attributesDB1 = fqaStringsForEachDB.get(1);
            if (!attributesDB1.trim().isEmpty()) {
                String statementForDB1 = "SELECT " + fqaStringsForEachDB.get(1) + " " + stmtStartWithFromForDB1;
                ResultSet resultSetForDB1 = getLocalResultSet(statementForDB1, 1);
                resultSets.put(1, resultSetForDB1);
            }

            String attributesDB2 = fqaStringsForEachDB.get(2);
            if (!attributesDB2.trim().isEmpty()) {
                String statementForDB2 = "SELECT " + fqaStringsForEachDB.get(2) + " " + stmtStartWithFromForDB2;
                ResultSet resultSetForDB2 = getLocalResultSet(statementForDB2, 2);
                resultSets.put(2, resultSetForDB2);
            }
            String attributesDB3 = fqaStringsForEachDB.get(3);
            if (!attributesDB3.trim().isEmpty()) {
                String statementForDB3 = "SELECT " + fqaStringsForEachDB.get(3) + " " + stmtStartWithFromForDB3;
                ResultSet resultSetForDB3 = getLocalResultSet(statementForDB3, 3);
                resultSets.put(3, resultSetForDB3);
            }
        } catch (FedException ex) {
            for (ResultSet rs : resultSets.values()) {
                try {
                    if (!rs.isClosed()) {
                        try (java.sql.Statement resultSetStatement = rs.getStatement()) {
                            rs.close();
                            DatabaseCursorChecker.closeResultSet(this.getClass());
                        } finally {
                            DatabaseCursorChecker.closeStatement(this.getClass());
                        }
                    }
                } catch (SQLException ex2) {
                    //ignore exception
                }
            }
            throw ex;
        }

        return resultSets;
    }

    protected HashMap<Integer, ResultSet> getOtherResultSets(ResultSetHelpInfo helper, String pkValue, List<Integer> checkedDBs)
            throws FedException {

        Table table = helper.getTable();
        String stmtOtherDbAfterFrom = helper.getStmtAfterFrom();

        ArrayList<String> params = (ArrayList<String>) helper.getParams().clone();
        if (pkValue != null && !pkValue.isEmpty()) {
            params.add(pkValue);
        }

        stmtOtherDbAfterFrom = String.format(stmtOtherDbAfterFrom, params.toArray());

        HashMap<Integer, ResultSet> resultSetsForOtherDB = new HashMap<>();
        try {
            HashMap<Integer, String> fqAttributesForEachDB = helper.getFqAttributesForEachDB();
            if (fqAttributesForEachDB.isEmpty()) {
                String finalStatement = "SELECT * " + stmtOtherDbAfterFrom;
                HashMap<Integer, LinkedHashMap<String, Column>> dbRelatedColumns = table.getDatabaseRelatedColumns();
                for (Map.Entry<Integer, LinkedHashMap<String, Column>> entry : dbRelatedColumns.entrySet()) {
                    if (!(checkedDBs.contains(entry.getKey()) || entry.getValue().isEmpty())) {
                        int dbId = entry.getKey();
                        ResultSet rs = getLocalResultSet(finalStatement, dbId);
                        resultSetsForOtherDB.put(dbId, rs);
                    }
                }
            } else {
                for (Map.Entry<Integer, String> entry : fqAttributesForEachDB.entrySet()) {
                    if (!(checkedDBs.contains(entry.getKey()) || entry.getValue().isEmpty())) {
                        String finalStatement = String.format("SELECT %s %s", entry.getValue(), stmtOtherDbAfterFrom);
                        ResultSet rs = getLocalResultSet(finalStatement, entry.getKey());
                        resultSetsForOtherDB.put(entry.getKey(), rs);
                    }
                }
            }
        } catch (FedException ex) {
            for (ResultSet rs : resultSetsForOtherDB.values()) {
                try {
                    if (!rs.isClosed()) {
                        try (java.sql.Statement resultSetStatement = rs.getStatement()) {
                            rs.close();
                            DatabaseCursorChecker.closeResultSet(this.getClass());
                        } finally {
                            DatabaseCursorChecker.closeStatement(this.getClass());
                        }
                    }
                } catch (SQLException ex2) {
                    //ignore exception
                }
            }
            throw ex;
        }
        return resultSetsForOtherDB;
    }

    protected String getCorrectStatement(Table table, String attName, String intStmt, String vCharStmt) {
        String correctStatement;

        Column column = table.getColumn(attName);
        String columnType = column.getType();

        if (columnType.equals("INTEGER")) {
            correctStatement = intStmt;
        } else {
            correctStatement = vCharStmt;
        }

        return correctStatement;
    }

    protected int getAmountOfNestedWheres(BinaryExpression binaryExpression) {
        int amountOfBE = 1;
        if (binaryExpression.isANestedBinaryExpression()) {
            amountOfBE = 2;
            if (((BinaryExpression) binaryExpression.getRightOperand()).isANestedBinaryExpression()) {
                amountOfBE = 3;
            }
        }
        return amountOfBE;
    }
}
