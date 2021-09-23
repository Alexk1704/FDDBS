package fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinornonjoin;

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
import fdbs.sql.parser.ast.statement.Statement;
import fdbs.sql.parser.ast.statement.select.SelectNoGroupStatement;
import fdbs.util.DatabaseCursorChecker;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class NoGroupWithNonJoinOrNonJoinVerticalResultSet extends FedResultSet {

    private SelectNoGroupStatement selectStatement;

    private Table table = null;
    private String primaryKeyColumnName = null;
    private ArrayList<String> rowAttributes = new ArrayList<>();
    private HashMap<Integer, ResultSetMetaData> metaDatas = null;
    private HashMap<Integer, ColumnAccessInfo> indexColumnAccessInfoMapping = null;
    private HashMap<String, ColumnAccessInfo> internalNameColumnAccessInfoMapping = null;

    private ResultSet localVerticalResultSet;
    private HashMap<Integer, ResultSet> otherResultSets = new HashMap<>();
    private HashMap<Integer, ResultSet> localRowResultSets = new HashMap<>();

    private int firstRequestingDB = 0;
    private ResultSetHelpInfo helper;

    private boolean forcedNext = false;
    private boolean forcedNextValue = false;

    private String leftBinaryExpressionAttributeName = null;
    private String leftBinaryExpressionAttributeType = null;
    private BinaryExpression.Operator leftBinaryExpressionOperator = null;
    private String leftBinaryExpressionConstant = null;

    private String rightBinaryExpressionAttributeName = null;
    private String rightBinaryExpressionAttributeType = null;
    private BinaryExpression.Operator rightBinaryExpressionOperator = null;
    private String rightBinaryExpressionConstant = null;

    public NoGroupWithNonJoinOrNonJoinVerticalResultSet(FedConnection fedConnection, Statement statement)
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

        while (localVerticalResultSet.next()) {
            boolean tempNext = false;
            localRowResultSets.clear();
            if (!otherResultSets.isEmpty()) {
                closeOtherResultSets();
            }

            otherResultSets = getOtherResultSets(helper, localVerticalResultSet.getString(primaryKeyColumnName), new ArrayList<Integer>() {
                {
                    for (int i = 1; i <= firstRequestingDB; i++) {
                        add(i);
                    }
                }
            });

            localRowResultSets.put(firstRequestingDB, localVerticalResultSet);
            otherResultSets.entrySet().forEach((e) -> {
                localRowResultSets.put(e.getKey(), e.getValue());
            });

            if (otherResultSets.isEmpty()) {
                tempNext = true;
            } else {
                for (ResultSet rs : otherResultSets.values()) {
                    if (rs.next()) {
                        tempNext = true;
                    } else if (tempNext == true) { // and rs.next() == false
                        throw new FedException(String.format("%s: Unexpceted local results. Not all local result sets have a result.",
                                this.getClass().getSimpleName()));
                    }
                }
            }

            metaDatas = initialiseMetaDatas(localRowResultSets);

            if (indexColumnAccessInfoMapping == null) {
                indexColumnAccessInfoMapping = getColumnAccessInfo(metaDatas, rowAttributes);
            }

            if (internalNameColumnAccessInfoMapping == null) {
                initializeInternalNameColumnAccessMapping();
            }

            if (tempNext == false) {
                break;
            } else if (isCurrentRowValidAgainstConditions()) {
                next = tempNext;
                break;
            }
        }

        return next;
    }

    private void initializeInternalNameColumnAccessMapping() throws FedException {
        internalNameColumnAccessInfoMapping = new HashMap<>();

        for (Map.Entry<Integer, ResultSetMetaData> entry : metaDatas.entrySet()) {
            int dbId = entry.getKey();
            ResultSetMetaData metaData = entry.getValue();

            try {
                int columnsCount = metaData.getColumnCount();
                for (int rsIndex = 1; rsIndex <= columnsCount; rsIndex++) {
                    String columnName = metaData.getColumnName(rsIndex);
                    if (!internalNameColumnAccessInfoMapping.containsKey(columnName)) {
                        internalNameColumnAccessInfoMapping.put(columnName, new ColumnAccessInfo(dbId, rsIndex));
                    }
                }
            } catch (SQLException ex) {
                throw new FedException(String.format("%s: An error occurred while retrieving column infos form result set metadata.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
            }
        }
    }

    private boolean isCurrentRowValidAgainstConditions() throws Exception {
        boolean isValid = false;

        if (isCurrentRowValidAgainstCondition(leftBinaryExpressionAttributeName, leftBinaryExpressionAttributeType, leftBinaryExpressionOperator, leftBinaryExpressionConstant)
                || isCurrentRowValidAgainstCondition(rightBinaryExpressionAttributeName, rightBinaryExpressionAttributeType, rightBinaryExpressionOperator, rightBinaryExpressionConstant)) {
            isValid = true;
        }

        return isValid;
    }

    private boolean isCurrentRowValidAgainstCondition(String attributeName, String attributeType, BinaryExpression.Operator operator, String constant) throws Exception {
        boolean isValid = false;

        ColumnAccessInfo columnAccessInfo = internalNameColumnAccessInfoMapping.get(attributeName);

        if (attributeType.equals("INTEGER")) {
            ResultSet rs = localRowResultSets.get(columnAccessInfo.getResultSetDbId());
            int leftValue = rs.getInt(columnAccessInfo.getResultSetIndex());
            if (!rs.wasNull()) {
                int rightValue = Integer.parseInt(constant);
                switch (operator) {
                    case EQ:
                        isValid = leftValue == rightValue;
                        break;
                    case GT:
                        isValid = leftValue > rightValue;
                        break;
                    case LT:
                        isValid = leftValue < rightValue;
                        break;
                    case GEQ:
                        isValid = leftValue >= rightValue;
                        break;
                    case LEQ:
                        isValid = leftValue <= rightValue;
                        break;
                    case NEQ:
                        isValid = leftValue != rightValue;
                        break;
                }
            }
        } else {
            ResultSet rs = localRowResultSets.get(columnAccessInfo.getResultSetDbId());
            String leftValue = rs.getString(columnAccessInfo.getResultSetIndex());

            if (!rs.wasNull()) {
                // Zahlen < Buchstaben < Kleinbuchstaben
                switch (operator) {
                    case EQ:
                        isValid = leftValue.equals(constant);
                        break;
                    case GT:
                        isValid = leftValue.compareTo(constant) > 0;
                        break;
                    case LT:
                        isValid = leftValue.compareTo(constant) < 0;
                        break;
                    case GEQ:
                        isValid = leftValue.compareTo(constant) > 0 || leftValue.equals(constant);
                        break;
                    case LEQ:
                        isValid = leftValue.compareTo(constant) < 0 || leftValue.equals(constant);
                        break;
                    case NEQ:
                        isValid = !leftValue.equals(constant);
                        break;
                }
            }
        }

        return isValid;
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
        rowAttributes.clear();
        metaDatas = null;
        internalNameColumnAccessInfoMapping = null;
        indexColumnAccessInfoMapping = null;

        firstRequestingDB = 0;

        forcedNext = false;
        forcedNextValue = false;

        leftBinaryExpressionAttributeName = null;
        leftBinaryExpressionAttributeType = null;
        leftBinaryExpressionOperator = null;
        leftBinaryExpressionConstant = null;

        rightBinaryExpressionAttributeName = null;
        rightBinaryExpressionAttributeType = null;
        rightBinaryExpressionOperator = null;
        rightBinaryExpressionConstant = null;

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

        BinaryExpression nestedBinaryExpression = selectStatement.getWhereClause().getBinaryExpression();
        BinaryExpression leftBinaryExpression = (BinaryExpression) nestedBinaryExpression.getLeftOperand();
        BinaryExpression rightBinaryExpression = (BinaryExpression) nestedBinaryExpression.getRightOperand();

        FullyQualifiedAttributeIdentifier leftBinaryExpressionAttribute = (FullyQualifiedAttributeIdentifier) leftBinaryExpression.getLeftOperand();
        FullyQualifiedAttributeIdentifier rightBinaryExpressionAttribute = (FullyQualifiedAttributeIdentifier) rightBinaryExpression.getLeftOperand();

        leftBinaryExpressionAttributeName = leftBinaryExpressionAttribute.getAttributeIdentifier().getIdentifier();
        Column leftAttributeColumn = table.getColumn(leftBinaryExpressionAttributeName);

        leftBinaryExpressionAttributeType = leftAttributeColumn.getType();
        leftBinaryExpressionOperator = leftBinaryExpression.getOperator();
        leftBinaryExpressionConstant = leftBinaryExpression.getRightOperand().getIdentifier();

        rightBinaryExpressionAttributeName = rightBinaryExpressionAttribute.getAttributeIdentifier().getIdentifier();
        Column righttAttributeColumn = table.getColumn(rightBinaryExpressionAttributeName);
        rightBinaryExpressionAttributeType = righttAttributeColumn.getType();
        rightBinaryExpressionOperator = rightBinaryExpression.getOperator();
        rightBinaryExpressionConstant = rightBinaryExpression.getRightOperand().getIdentifier();

        String queryStatement;
        if (hasFullyQualifiedAttributes()) {
            List<FullyQualifiedAttributeIdentifier> fqAttributes = selectStatement.getFQAttributeIdentifiers();
            initializeFQRowAttributes(fqAttributes);

            if (!isFQAttributeRequestedInSelect(leftBinaryExpressionAttribute.getFullyQualifiedIdentifier(), fqAttributes)) {
                fqAttributes.add(leftBinaryExpressionAttribute);
            }

            if (!isFQAttributeRequestedInSelect(rightBinaryExpressionAttribute.getFullyQualifiedIdentifier(), fqAttributes)) {
                fqAttributes.add(rightBinaryExpressionAttribute);
            }

            HashMap<Integer, String> fqAttributesForEachDB = getFQAttributesForEachDB(table, fqAttributes);
            initializeFirstRequestingDB(fqAttributesForEachDB);
            if (firstRequestingDB == 0) {
                throw new FedException(String.format("%s: An error occurred while resolving first requesting database.",
                        this.getClass().getSimpleName()));
            }

            String fqAttributesForEachDB1 = fqAttributesForEachDB.get(firstRequestingDB);
            String fullyQualifiedPrimaryKey = String.format("%s.%s", tableName, primaryKeyColumnName);
            if (isFQAttributeRequestedInSelect(fullyQualifiedPrimaryKey, getFQAttributesForEachDbAsList(table, fqAttributes).get(firstRequestingDB))) {
                queryStatement = String.format("SELECT %s FROM %s", fqAttributesForEachDB1, tableName);
            } else {
                queryStatement = String.format("SELECT %s,%s FROM %s", fqAttributesForEachDB1, fullyQualifiedPrimaryKey, tableName);
            }
            initializeResultSetHelper(tableName, fqAttributesForEachDB);
        } else {
            firstRequestingDB = 1;
            initializeAllRowAttributes();
            queryStatement = String.format("SELECT * FROM %s ", tableName);
            initializeResultSetHelper(tableName, null);
        }

        localVerticalResultSet = getLocalResultSet(queryStatement, firstRequestingDB);
    }

    private boolean hasFullyQualifiedAttributes() {
        return selectStatement.getFQAttributeIdentifiers() != null && !selectStatement.getFQAttributeIdentifiers().isEmpty();
    }

    private void initializeFirstRequestingDB(HashMap<Integer, String> fqAttributesForEachDB) {
        int requestingDB = 0;

        for (int i = 1; i <= fqAttributesForEachDB.size(); i++) {
            if (fqAttributesForEachDB.containsKey(i) && !fqAttributesForEachDB.get(i).isEmpty()) {
                requestingDB = i;
                break;
            }
        }

        firstRequestingDB = requestingDB;
    }

    private void initializeFQRowAttributes(List<FullyQualifiedAttributeIdentifier> fqAttributes) {
        for (FullyQualifiedAttributeIdentifier attribute : fqAttributes) {
            String attributeName = attribute.getAttributeIdentifier().getIdentifier();
            rowAttributes.add(attributeName);
        }
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
        ResultSetHelpInfo forcedHelper = new ResultSetHelpInfo(helper.getFqAttributesForEachDB(), table, "FROM %s", new ArrayList<String>() {
            {
                add(table.getAttributeName());
            }
        });
        otherResultSets = getOtherResultSets(forcedHelper, null, new ArrayList<Integer>() {
            {
                for (int i = 1; i <= firstRequestingDB; i++) {
                    add(i);
                }
            }
        });

        localRowResultSets.put(firstRequestingDB, localVerticalResultSet);
        otherResultSets.entrySet().forEach((e) -> {
            localRowResultSets.put(e.getKey(), e.getValue());
        });

        metaDatas = initialiseMetaDatas(localRowResultSets);
        indexColumnAccessInfoMapping = getColumnAccessInfo(metaDatas, rowAttributes);
    }

    private boolean isFQAttributeRequestedInSelect(String fqAttributeAsString, List<FullyQualifiedAttributeIdentifier> fqSelectAttributes) {
        boolean isRequested = false;

        for (FullyQualifiedAttributeIdentifier attribute : fqSelectAttributes) {
            if (attribute.getFullyQualifiedIdentifier().equals(fqAttributeAsString)) {
                isRequested = true;
                break;
            }
        }

        return isRequested;
    }
}
