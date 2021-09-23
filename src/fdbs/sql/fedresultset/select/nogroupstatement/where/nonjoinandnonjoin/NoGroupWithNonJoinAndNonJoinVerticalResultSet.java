package fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinandnonjoin;

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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public final class NoGroupWithNonJoinAndNonJoinVerticalResultSet extends FedResultSet {

    private SelectNoGroupStatement selectStatement;
    private int sameDB;

    private Table table = null;
    private int conditionAttributeDb1 = 0;
    private int conditionAttributeDb2 = 0;
    private String primaryKeyColumnName = null;

    private ArrayList<String> rowAttributes = new ArrayList<>();
    private HashMap<Integer, ResultSetMetaData> metaDatas = null;
    private HashMap<Integer, ColumnAccessInfo> indexColumnAccessInfoMapping = null;

    private ResultSetHelpInfo primaryKeyCheckHelper = null;
    private ResultSetHelpInfo secondConditionHelper = null;

    private HashMap<Integer, ResultSet> secondConditionResultSets = new HashMap<>();
    private ResultSet firstConditionResultSet = null;

    private HashMap<Integer, ResultSet> otherResultSets = new HashMap<>();
    private HashMap<Integer, ResultSet> localRowResultSets = new HashMap<>();

    private boolean forcedNext = false;
    private boolean forcedNextValue = false;

    public NoGroupWithNonJoinAndNonJoinVerticalResultSet(FedConnection fedConnection, Statement statement, int sameDB) throws FedException {
        super(fedConnection, statement);
        selectStatement = (SelectNoGroupStatement) statement;
        this.sameDB = sameDB;
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
        while (firstConditionResultSet.next()) {
            localRowResultSets.clear();
            closeSecondConditionResultSets();
            if (!otherResultSets.isEmpty()) {
                closeOtherResultSets();
            }

            if (sameDB > 0) {
                otherResultSets = getOtherResultSets(primaryKeyCheckHelper, firstConditionResultSet.getString(primaryKeyColumnName), new ArrayList<Integer>() {
                    {
                        add(conditionAttributeDb1);
                    }
                });

                localRowResultSets.put(conditionAttributeDb1, firstConditionResultSet);

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
            } else {
                secondConditionResultSets = getOtherResultSets(secondConditionHelper, firstConditionResultSet.getString(primaryKeyColumnName), new ArrayList<Integer>() {
                    {
                        add(0);
                    }
                });
                ResultSet secondConditionResultSet = secondConditionResultSets.get(conditionAttributeDb2);

                localRowResultSets.put(conditionAttributeDb1, firstConditionResultSet);
                localRowResultSets.put(conditionAttributeDb2, secondConditionResultSet);
                boolean requestingDB3 = table.getDatabaseRelatedColumns().size() == 3;

                if (requestingDB3) {
                    otherResultSets = getOtherResultSets(primaryKeyCheckHelper, firstConditionResultSet.getString(primaryKeyColumnName), new ArrayList<Integer>() {
                        {
                            add(conditionAttributeDb1);
                            add(conditionAttributeDb2);
                        }
                    });
                    otherResultSets.entrySet().forEach((e) -> {
                        localRowResultSets.put(e.getKey(), e.getValue());
                    });
                }

                if (secondConditionResultSet.next()) {
                    if (requestingDB3) {
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
                    } else {
                        next = true;
                    }
                }
            }

            metaDatas = initialiseMetaDatas(localRowResultSets);

            if (indexColumnAccessInfoMapping == null) {
                indexColumnAccessInfoMapping = getColumnAccessInfo(metaDatas, rowAttributes);
            }

            if (next == true) {
                break;
            }
        }

        return next;
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
        List<Exception> occuredExceptions = new ArrayList<>();

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

        for (ResultSet rs : secondConditionResultSets.values()) {
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

        table = null;
        conditionAttributeDb1 = 0;
        conditionAttributeDb2 = 0;
        primaryKeyColumnName = null;

        rowAttributes.clear();
        metaDatas = null;
        indexColumnAccessInfoMapping = null;

        primaryKeyCheckHelper = null;
        secondConditionHelper = null;

        firstConditionResultSet = null;

        otherResultSets = new HashMap<>();
        localRowResultSets = new HashMap<>();

        forcedNext = false;
        forcedNextValue = false;

        if (occuredExceptions.size()
                > 0) {
            fedException = new FedException(String.format("%s: An error occurred while closing local result sets.%n%s",
                    this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));
            for (Exception exception : occuredExceptions) {
                fedException.addSuppressed(exception);
            }
        }

        return fedException;
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
            FedException fedException = new FedException(String.format("%s: An error occurred while closing local result sets.\n%s",
                    this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));
            for (Exception exception : occuredExceptions) {
                fedException.addSuppressed(exception);
                throw fedException;
            }

            throw fedException;
        }

    }

    private void closeSecondConditionResultSets() throws FedException {
        List<SQLException> occuredExceptions = new ArrayList<>();
        if (secondConditionResultSets != null) {
            for (ResultSet rs : secondConditionResultSets.values()) {
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
                FedException fedException = new FedException(String.format("%s: An error occurred while closing local result sets.\n%s",
                        this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));
                for (Exception exception : occuredExceptions) {
                    fedException.addSuppressed(exception);
                    throw fedException;
                }

                throw fedException;
            }
        }

    }

    @Override
    protected void resolveStatement() throws FedException {
        String tableName = selectStatement.getTableIdentifiers().get(0).getIdentifier();
        table = metadataManager.getTable(tableName);
        primaryKeyColumnName = table.getPrimaryKeyConstraints().get(0).getAttributeName();

        BinaryExpression binaryExpression = (BinaryExpression) selectStatement.getWhereClause().getBinaryExpression()
                .getLeftOperand();
        FullyQualifiedAttributeIdentifier leftOperandFQ = (FullyQualifiedAttributeIdentifier) binaryExpression
                .getLeftOperand();
        String leftOperand = leftOperandFQ.getAttributeIdentifier().getIdentifier();

        if (sameDB > 0) {
            conditionAttributeDb1 = sameDB;
        } else {
            conditionAttributeDb1 = table.getColumn(leftOperand).getDatabase();
        }

        if (hasFullyQualifiedAttributes()) {
            List<FullyQualifiedAttributeIdentifier> fqAttributes = selectStatement.getFQAttributeIdentifiers();
            initializeFQRowAttributes(fqAttributes);
            HashMap<Integer, String> fqAttributesForEachDB = getFQAttributesForEachDB(table, fqAttributes);

            boolean containsPrimaryKey = checkFullyQualifiedAttributesForPK(fqAttributes);
            String firstConditionStatement;
            if (containsPrimaryKey) {
                if (sameDB > 0) {
                    firstConditionStatement = String.format("SELECT %s FROM %s WHERE %s AND %s",
                            fqAttributesForEachDB.get(conditionAttributeDb1), tableName,
                            getFirstBinaryExpression(), getSecondBinaryExpression());
                } else {
                    firstConditionStatement = String.format("SELECT %s FROM %s WHERE %s",
                            fqAttributesForEachDB.get(conditionAttributeDb1), tableName, getFirstBinaryExpression());
                    initializeResultSetHelperSecondCondition(tableName, fqAttributesForEachDB);
                }
            } else {
                String fullyQualifiedPrimaryKey = String.format("%s.%s", tableName, primaryKeyColumnName);
                String conditionAttributesDB1AttributeList = fqAttributesForEachDB.get(conditionAttributeDb1);
                String stringifyFQAttributes;
                if (conditionAttributesDB1AttributeList.isEmpty()) {
                    stringifyFQAttributes = fullyQualifiedPrimaryKey;
                } else {
                    stringifyFQAttributes = String.format("%s,%s", conditionAttributesDB1AttributeList, fullyQualifiedPrimaryKey);
                }

                if (sameDB > 0) {
                    firstConditionStatement = String.format("SELECT %s FROM %s WHERE %s AND %s",
                            stringifyFQAttributes, tableName, getFirstBinaryExpression(), getSecondBinaryExpression());
                } else {
                    firstConditionStatement = String.format("SELECT %s FROM %s WHERE %s",
                            stringifyFQAttributes, tableName, getFirstBinaryExpression());
                    initializeResultSetHelperSecondCondition(tableName, fqAttributesForEachDB);
                }
            }

            initializeResultSetHelperPrimaryKeyCheck(tableName, fqAttributesForEachDB);
            firstConditionResultSet = getLocalResultSet(firstConditionStatement, conditionAttributeDb1);
        } else {
            initializeAllRowAttributes();
            String firstConditionStatement;

            if (sameDB > 0) {
                conditionAttributeDb1 = sameDB;
                firstConditionStatement = String.format("SELECT * FROM %s WHERE %s AND %s",
                        tableName, getFirstBinaryExpression(), getSecondBinaryExpression());
            } else {
                firstConditionStatement = String.format("SELECT * FROM %s WHERE %s", tableName, getFirstBinaryExpression());
                initializeResultSetHelperSecondCondition(tableName, null);
            }

            initializeResultSetHelperPrimaryKeyCheck(tableName, null);
            firstConditionResultSet = getLocalResultSet(firstConditionStatement, conditionAttributeDb1);
        }
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

    private void initializeAllRowAttributes() {
        ArrayList<Column> dbColumns = table.getColumns();
        for (Column c : dbColumns) {
            String attribute = c.getAttributeName();
            rowAttributes.add(attribute);
        }
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

    private void initializeResultSetHelperPrimaryKeyCheck(String tableName, HashMap<Integer, String> fqAttributesForEachDB) {
        String fromStatement;

        if (table.getColumn(primaryKeyColumnName).getType().equals("INTEGER")) {
            fromStatement = "FROM %s WHERE (%s = %s)";
        } else {
            fromStatement = "FROM %s WHERE (%s = '%s')";
        }

        ArrayList<String> paramList = new ArrayList();
        paramList.add(table.getAttributeName());
        paramList.add(tableName + "." + primaryKeyColumnName);

        if (fqAttributesForEachDB == null) {
            primaryKeyCheckHelper = new ResultSetHelpInfo(table, fromStatement, paramList);
        } else {
            primaryKeyCheckHelper = new ResultSetHelpInfo(fqAttributesForEachDB, table, fromStatement, paramList);
        }
    }

    private void initializeResultSetHelperSecondCondition(String tableName, HashMap<Integer, String> fqAttributesForEachDB) throws FedException {
        String fromStatement;

        BinaryExpression binaryExpression = (BinaryExpression) selectStatement.getWhereClause().getBinaryExpression()
                .getRightOperand();
        FullyQualifiedAttributeIdentifier leftOperandFQ
                = (FullyQualifiedAttributeIdentifier) binaryExpression.getLeftOperand();
        String leftOperand = leftOperandFQ.getFullyQualifiedIdentifier();
        String operator = binaryExpression.getSqlOperatorName();
        String rightOperand = binaryExpression.getRightOperand().getIdentifier();

        Column conditionAttributeColumn = table.getColumn(leftOperandFQ.getAttributeIdentifier().getIdentifier());
        conditionAttributeDb2 = conditionAttributeColumn.getDatabase();
        if (conditionAttributeDb1 == conditionAttributeDb2) {
            throw new FedException(String.format("%s: An error occurred while resolving conditions databases.", this.getClass().getSimpleName()));
        }

        ArrayList<String> paramList = new ArrayList();
        if (conditionAttributeColumn.getType().equals("INTEGER")) {
            fromStatement = "FROM %s WHERE (%s %s %s)";
        } else {
            fromStatement = "FROM %s WHERE (%s %s '%s')";
        }

        paramList.add(table.getAttributeName());
        paramList.add(leftOperand);
        paramList.add(operator);
        paramList.add(rightOperand);

        if (table.getColumn(primaryKeyColumnName).getType().equals("INTEGER")) {
            fromStatement += " and (%s = %s)";
        } else {
            fromStatement += " and (%s = '%s')";
        }

        paramList.add(tableName + "." + primaryKeyColumnName);
        HashMap<Integer, String> fqAttributesForSecondConditionDB = new HashMap<>();
        if (fqAttributesForEachDB == null) {
            fqAttributesForSecondConditionDB.put(conditionAttributeDb2, "*");
            secondConditionHelper = new ResultSetHelpInfo(fqAttributesForSecondConditionDB, table, fromStatement, paramList);
        } else {
            String stringifyAttributes = fqAttributesForEachDB.get(conditionAttributeDb2);
            if (stringifyAttributes == null || stringifyAttributes.isEmpty()) {
                fqAttributesForSecondConditionDB.put(conditionAttributeDb2, tableName + "." + primaryKeyColumnName);
            } else {
                fqAttributesForSecondConditionDB.put(conditionAttributeDb2, stringifyAttributes);
            }

            secondConditionHelper = new ResultSetHelpInfo(fqAttributesForSecondConditionDB, table, fromStatement, paramList);
        }
    }

    private boolean checkFullyQualifiedAttributesForPK(List<FullyQualifiedAttributeIdentifier> fqAttributes) {
        HashMap<Integer, List<FullyQualifiedAttributeIdentifier>> fqAttributeListForEachDb = getFQAttributesForEachDbAsList(table, fqAttributes);
        List<FullyQualifiedAttributeIdentifier> fqaiList = fqAttributeListForEachDb.get(conditionAttributeDb1);
        boolean containsPrimaryKey = false;
        for (FullyQualifiedAttributeIdentifier fqai : fqaiList) {
            if (fqai.getAttributeIdentifier().getIdentifier().equals(primaryKeyColumnName)) {
                containsPrimaryKey = true;
                break;
            }
        }
        return containsPrimaryKey;
    }

    private void forceMetaDataInitialization() throws Exception {
        localRowResultSets.clear();
        if (!otherResultSets.isEmpty()) {
            closeOtherResultSets();
        }

        if (sameDB > 0) {
            ResultSetHelpInfo forcedHelper = new ResultSetHelpInfo(primaryKeyCheckHelper.getFqAttributesForEachDB(), table, "FROM %s", new ArrayList<String>() {
                {
                    add(table.getAttributeName());
                }
            });
            otherResultSets = getOtherResultSets(forcedHelper, null, new ArrayList<Integer>() {
                {
                    add(conditionAttributeDb1);
                }
            });

            localRowResultSets.put(conditionAttributeDb1, firstConditionResultSet);
            otherResultSets.entrySet().forEach((e) -> {
                localRowResultSets.put(e.getKey(), e.getValue());
            });
        } else {
            ResultSetHelpInfo forcedSecondCondtionHelper = new ResultSetHelpInfo(secondConditionHelper.getFqAttributesForEachDB(), table, "FROM %s", new ArrayList<String>() {
                {
                    add(table.getAttributeName());
                }
            });
            HashMap<Integer, ResultSet> secondConditionResultSets = getOtherResultSets(forcedSecondCondtionHelper, null, new ArrayList<Integer>() {
                {
                    add(conditionAttributeDb1);
                }
            });
            ResultSet secondConditionResultSet = secondConditionResultSets.get(conditionAttributeDb2);

            localRowResultSets.put(conditionAttributeDb1, firstConditionResultSet);
            localRowResultSets.put(conditionAttributeDb2, secondConditionResultSet);
            boolean requestingDB3 = table.getDatabaseRelatedColumns().size() == 3;

            if (requestingDB3) {
                ResultSetHelpInfo forcedOtherHelper = new ResultSetHelpInfo(primaryKeyCheckHelper.getFqAttributesForEachDB(), table, "FROM %s", new ArrayList<String>() {
                    {
                        add(table.getAttributeName());
                    }
                });
                otherResultSets = getOtherResultSets(forcedOtherHelper, null, new ArrayList<Integer>() {
                    {
                        add(conditionAttributeDb1);
                        add(conditionAttributeDb2);
                    }
                });

                otherResultSets.entrySet().forEach((e) -> {
                    localRowResultSets.put(e.getKey(), e.getValue());
                });
            }
        }

        metaDatas = initialiseMetaDatas(localRowResultSets);
        indexColumnAccessInfoMapping = getColumnAccessInfo(metaDatas, rowAttributes);
    }
}
