package fdbs.sql.fedresultset.select.nogroupstatement.where.join;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.fedresultset.FedResultSetAccessInfo;
import fdbs.sql.fedresultset.select.nogroupstatement.*;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin.*;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinandnonjoin.*;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinornonjoin.*;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.PartitionType;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.clause.WhereClause;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.literal.Literal;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.literal.StringLiteral;
import fdbs.sql.parser.ast.statement.Statement;
import fdbs.sql.parser.ast.statement.select.SelectNoGroupStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class NoGroupWithJoinResultSet extends FedResultSet {

    private SelectNoGroupStatement selectStatement;

    private TableIdentifier table1Identifier = null;
    private TableIdentifier table2Identifier = null;

    private List<FullyQualifiedAttributeIdentifier> rowFQAttributesForJoinedTable = null;

    private ArrayList<String> internalRowAttributesForTable1 = null;
    private ArrayList<String> internalRowAttributesForTable2 = null;

    private SelectNoGroupStatement table2NoGroupStatement = null;

    private FedResultSet table1FedResultSet = null;
    private FedResultSet currentTable2FedResultSet = null;

    // Join Informations
    private String joinType = null;
    private FullyQualifiedAttributeIdentifier joinAttributeTable1 = null;
    private FullyQualifiedAttributeIdentifier joinAttributeTable2 = null;
    private BinaryExpression.Operator joinOperator = null;

    // Non Join 1 Informations
    private FullyQualifiedAttributeIdentifier binaryExpressionAttribute1 = null;
    private String binaryExpressionAttributeType1 = null;
    private BinaryExpression.Operator binaryExpressionOperator1 = null;
    private String binaryExpressionConstant1 = null;

    // Non Joins Operator
    private BinaryExpression.Operator nonJoinsOperator = null;

    // Non Join 2 Informations
    private FullyQualifiedAttributeIdentifier binaryExpressionAttribute2 = null;
    private String binaryExpressionAttributeType2 = null;
    private BinaryExpression.Operator binaryExpressionOperator2 = null;
    private String binaryExpressionConstant2 = null;

    private List<BinaryExpression> table1NonJoinBinaryExpressions = null;

    private boolean hasCurrentRow = false;

    private boolean forcedNext = false;
    private boolean forcedNextValue = false;

    public NoGroupWithJoinResultSet(FedConnection fedConnection, Statement statement) throws FedException {
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

    private boolean getNext() throws FedException {
        boolean next = false;
        boolean finishedTable1 = false;

        do {
            if (!hasCurrentRow) {
                hasCurrentRow = table1FedResultSet.next();
                finishedTable1 = !hasCurrentRow;

                if (finishedTable1) {
                    break;
                }

                if (currentTable2FedResultSet != null && !currentTable2FedResultSet.isClosed()) {
                    currentTable2FedResultSet.close();
                }
                
                table2NoGroupStatement.setWhereClause(getJoinWhere(table1FedResultSet.getString(internalRowAttributesForTable1.indexOf(joinAttributeTable1.getAttributeIdentifier().getIdentifier()) + 1)));
                currentTable2FedResultSet = getFedResultSet(table2NoGroupStatement);
                intializeInternalRowAttributesForTable2();
            }

            if (currentTable2FedResultSet.next()) {
                // check table 2 where    
                if (isCurrentRowValidAgainstConditions()) {
                    next = true;
                }
            } else {
                hasCurrentRow = false;
            }
        } while (next == false && finishedTable1 == false);

        return next;
    }

    private boolean isCurrentRowValidAgainstConditions() throws FedException {
        boolean isValid = false;

        if (nonJoinsOperator == null && binaryExpressionAttribute1 == null) {
            isValid = true;
        } else if (nonJoinsOperator == null && binaryExpressionAttribute1 != null) {
            // a not null condition must be only for table 2
            String leftContant = currentTable2FedResultSet.getString(internalRowAttributesForTable2.indexOf(binaryExpressionAttribute1.getAttributeIdentifier().getIdentifier()) + 1);
            isValid = isCurrentRowValidAgainstCondition(binaryExpressionAttributeType1, leftContant, binaryExpressionOperator1, binaryExpressionConstant1);
        } else if (nonJoinsOperator == BinaryExpression.Operator.OR) {
            // both conditions muss be != null
            ArrayList<String> condition1RowAttributes = binaryExpressionAttribute1.getIdentifier().equals(table1Identifier.getIdentifier()) ? internalRowAttributesForTable1 : internalRowAttributesForTable2;
            FedResultSet condition1FedResultSet = binaryExpressionAttribute1.getIdentifier().equals(table1Identifier.getIdentifier()) ? table1FedResultSet : currentTable2FedResultSet;

            String condition1LeftContant = condition1FedResultSet.getString(condition1RowAttributes.indexOf(binaryExpressionAttribute1.getAttributeIdentifier().getIdentifier()) + 1);
            isValid = isCurrentRowValidAgainstCondition(binaryExpressionAttributeType1, condition1LeftContant, binaryExpressionOperator1, binaryExpressionConstant1);

            if (!isValid) {
                ArrayList<String> condition2RowAttributes = binaryExpressionAttribute2.getIdentifier().equals(table1Identifier.getIdentifier()) ? internalRowAttributesForTable1 : internalRowAttributesForTable2;
                FedResultSet condition2FedResultSet = binaryExpressionAttribute2.getIdentifier().equals(table1Identifier.getIdentifier()) ? table1FedResultSet : currentTable2FedResultSet;

                String condition2LeftContant = condition2FedResultSet.getString(condition2RowAttributes.indexOf(binaryExpressionAttribute2.getAttributeIdentifier().getIdentifier()) + 1);
                isValid = isCurrentRowValidAgainstCondition(binaryExpressionAttributeType2, condition2LeftContant, binaryExpressionOperator2, binaryExpressionConstant2);
            }
        } else if (nonJoinsOperator == BinaryExpression.Operator.AND) {
            // a not null condition must be only for table 2
            boolean isCondition1Valid;
            boolean isCondition2Valid;

            if (binaryExpressionAttribute1 != null) {
                String leftContant = currentTable2FedResultSet.getString(internalRowAttributesForTable2.indexOf(binaryExpressionAttribute1.getAttributeIdentifier().getIdentifier()) + 1);
                isCondition1Valid = isCurrentRowValidAgainstCondition(binaryExpressionAttributeType1, leftContant, binaryExpressionOperator1, binaryExpressionConstant1);
            } else {
                isCondition1Valid = true;
            }

            if (binaryExpressionAttribute2 != null) {
                String leftContant = currentTable2FedResultSet.getString(internalRowAttributesForTable2.indexOf(binaryExpressionAttribute2.getAttributeIdentifier().getIdentifier()) + 1);
                isCondition2Valid = isCurrentRowValidAgainstCondition(binaryExpressionAttributeType2, leftContant, binaryExpressionOperator2, binaryExpressionConstant2);
            } else {
                isCondition2Valid = true;
            }

            isValid = isCondition1Valid && isCondition2Valid;
        }

        return isValid;
    }

    private boolean isCurrentRowValidAgainstCondition(String valueTypes, String leftConstant, BinaryExpression.Operator operator, String rightConstant) {
        boolean isValid = false;

        if (leftConstant != null && rightConstant != null) {
            isValid = false;
            if (valueTypes.equals("INTEGER")) {
                int leftValue = Integer.parseInt(leftConstant);
                int rightValue = Integer.parseInt(rightConstant);

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
            } else {
                // Zahlen < Buchstaben < Kleinbuchstaben
                switch (operator) {
                    case EQ:
                        isValid = leftConstant.equals(rightConstant);
                        break;
                    case GT:
                        isValid = leftConstant.compareTo(rightConstant) > 0;
                        break;
                    case LT:
                        isValid = leftConstant.compareTo(rightConstant) < 0;
                        break;
                    case GEQ:
                        isValid = leftConstant.compareTo(rightConstant) > 0 || leftConstant.equals(rightConstant);
                        break;
                    case LEQ:
                        isValid = leftConstant.compareTo(rightConstant) < 0 || leftConstant.equals(rightConstant);
                        break;
                    case NEQ:
                        isValid = !leftConstant.equals(rightConstant);
                        break;
                }
            }
        }

        return isValid;
    }

    @Override
    protected String getPrivateString(int columnIndex) throws Exception {
        FedResultSetAccessInfo fedResultAccessInfo = resolveColumnIndex(columnIndex);
        return fedResultAccessInfo.getResultSet().getString(fedResultAccessInfo.getColumnIndex());
    }

    @Override
    protected int getPrivateInt(int columnIndex) throws Exception {
        FedResultSetAccessInfo fedResultAccessInfo = resolveColumnIndex(columnIndex);
        return fedResultAccessInfo.getResultSet().getInt(fedResultAccessInfo.getColumnIndex());
    }

    @Override
    protected int getPrivateColumnCount() throws Exception {
        return rowFQAttributesForJoinedTable.size();
    }

    @Override
    protected String getPrivateColumnName(int columnIndex) throws Exception {
        if (currentTable2FedResultSet == null || internalRowAttributesForTable2 == null) {
            forcedNextValue = getNext();
            forcedNext = true;
            if (!forcedNextValue && (currentTable2FedResultSet == null || internalRowAttributesForTable2 == null)) {
                forcecurrentTable2FedResultSetInitialization();
            }
        }

        FedResultSetAccessInfo fedResultAccessInfo = resolveColumnIndex(columnIndex);
        return fedResultAccessInfo.getResultSet().getColumnName(fedResultAccessInfo.getColumnIndex());
    }

    @Override
    protected int getPrivateColumnType(int columnIndex) throws Exception {
        if (currentTable2FedResultSet == null || internalRowAttributesForTable2 == null) {
            forcedNextValue = getNext();
            forcedNext = true;
            if (!forcedNextValue && (currentTable2FedResultSet == null || internalRowAttributesForTable2 == null)) {
                forcecurrentTable2FedResultSetInitialization();
            }
        }

        FedResultSetAccessInfo fedResultAccessInfo = resolveColumnIndex(columnIndex);
        return fedResultAccessInfo.getResultSet().getColumnType(fedResultAccessInfo.getColumnIndex());
    }

    @Override
    protected FedException closeLocalResultSets() {
        FedException fedException = null;
        List<FedException> occuredExceptions = new ArrayList<>();

        try {
            if (!table1FedResultSet.isClosed()) {
                table1FedResultSet.close();
            }
        } catch (FedException ex) {
            occuredExceptions.add(ex);
        }

        try {
            if (currentTable2FedResultSet != null && !currentTable2FedResultSet.isClosed()) {
                currentTable2FedResultSet.close();
            }
        } catch (FedException ex) {
            occuredExceptions.add(ex);
        }

        //todo: reset all private fields
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
        SelectNoGroupStatement table1NoGroupStatement;

        List<TableIdentifier> tableIdentifiers = selectStatement.getTableIdentifiers();
        table1Identifier = tableIdentifiers.get(0);
        table2Identifier = tableIdentifiers.get(1);

        BinaryExpression whereExpression = selectStatement.getWhereClause().getBinaryExpression();
        if (whereExpression.isANestedBinaryExpression()) {
            initializeJoinInformations((BinaryExpression) whereExpression.getLeftOperand());
            initializeNonJoinConditionsInformations((BinaryExpression) whereExpression.getRightOperand());
        } else {
            initializeJoinInformations(whereExpression);
        }

        if (hasFullyQualifiedAttributes()) {
            List<FullyQualifiedAttributeIdentifier> fQattributeIdentifiersTable1 = new ArrayList<>();
            List<FullyQualifiedAttributeIdentifier> fQattributeIdentifiersTable2 = new ArrayList<>();

            rowFQAttributesForJoinedTable = selectStatement.getFQAttributeIdentifiers();

            if (!isFQAttributeRequestedInSelect(joinAttributeTable1, fQattributeIdentifiersTable1)) {
                fQattributeIdentifiersTable1.add(joinAttributeTable1);
            }

            for (FullyQualifiedAttributeIdentifier fqIdentifier : rowFQAttributesForJoinedTable) {
                if (fqIdentifier.getIdentifier().equals(table1Identifier.getIdentifier())) {
                    fQattributeIdentifiersTable1.add(fqIdentifier);
                } else {
                    fQattributeIdentifiersTable2.add(fqIdentifier);
                }
            }

            if (binaryExpressionAttribute1 != null
                    && binaryExpressionAttribute1.getIdentifier().equals(table1Identifier.getIdentifier())
                    && !isFQAttributeRequestedInSelect(binaryExpressionAttribute1, fQattributeIdentifiersTable1)) {
                fQattributeIdentifiersTable1.add(binaryExpressionAttribute1);

            } else if (binaryExpressionAttribute1 != null
                    && binaryExpressionAttribute1.getIdentifier().equals(table2Identifier.getIdentifier())
                    && !isFQAttributeRequestedInSelect(binaryExpressionAttribute1, fQattributeIdentifiersTable2)) {
                fQattributeIdentifiersTable2.add(binaryExpressionAttribute1);
            }

            if (binaryExpressionAttribute2 != null
                    && binaryExpressionAttribute2.getIdentifier().equals(table1Identifier.getIdentifier())
                    && !isFQAttributeRequestedInSelect(binaryExpressionAttribute2, fQattributeIdentifiersTable1)) {
                fQattributeIdentifiersTable1.add(binaryExpressionAttribute2);

            } else if (binaryExpressionAttribute2 != null
                    && binaryExpressionAttribute2.getIdentifier().equals(table2Identifier.getIdentifier())
                    && !isFQAttributeRequestedInSelect(binaryExpressionAttribute2, fQattributeIdentifiersTable2)) {
                fQattributeIdentifiersTable2.add(binaryExpressionAttribute2);
            }

            table1NoGroupStatement = new SelectNoGroupStatement(fQattributeIdentifiersTable1, Arrays.asList(table1Identifier), null);
            table2NoGroupStatement = new SelectNoGroupStatement(fQattributeIdentifiersTable2, Arrays.asList(table2Identifier), null);
        } else {
            initializeAllRowAttributes();
            table1NoGroupStatement = new SelectNoGroupStatement(selectStatement.getAllAttributeIdentifier(), Arrays.asList(table1Identifier), null);
            table2NoGroupStatement = new SelectNoGroupStatement(selectStatement.getAllAttributeIdentifier(), Arrays.asList(table2Identifier), null);
        }

        if (table1NonJoinBinaryExpressions != null && !table1NonJoinBinaryExpressions.isEmpty()) {
            WhereClause whereClause;
            if (table1NonJoinBinaryExpressions.size() == 1) {
                whereClause = new WhereClause(table1NonJoinBinaryExpressions.get(0));
            } else {
                // size == 2
                whereClause = new WhereClause((BinaryExpression) whereExpression.getRightOperand());
            }

            table1NoGroupStatement.setWhereClause(whereClause);
        }

        table1FedResultSet = getFedResultSet(table1NoGroupStatement);
        intializeInternalRowAttributesForTable1();
    }

    private boolean hasFullyQualifiedAttributes() {
        return selectStatement.getFQAttributeIdentifiers() != null && !selectStatement.getFQAttributeIdentifiers().isEmpty();
    }

    private void initializeJoinInformations(BinaryExpression joinBinaryExpression) throws FedException {
        FullyQualifiedAttributeIdentifier leftJoinAttribute = (FullyQualifiedAttributeIdentifier) joinBinaryExpression.getLeftOperand();
        FullyQualifiedAttributeIdentifier rightJoinAttribute = (FullyQualifiedAttributeIdentifier) joinBinaryExpression.getRightOperand();

        if (leftJoinAttribute.getIdentifier().equals(table1Identifier.getIdentifier())) {
            joinAttributeTable1 = leftJoinAttribute;
            joinAttributeTable2 = rightJoinAttribute;
        } else {
            joinAttributeTable1 = rightJoinAttribute;
            joinAttributeTable2 = leftJoinAttribute;
        }

        String tabel1Namme = table1Identifier.getIdentifier();
        Column joinAttribute1Column = metadataManager.getTable(tabel1Namme).getColumn(joinAttributeTable1.getAttributeIdentifier().getIdentifier());

        joinType = joinAttribute1Column.getType();
        joinOperator = getSwapBinaryExpressionOperator(joinBinaryExpression.getOperator());
    }

    private void initializeNonJoinConditionsInformations(BinaryExpression nonBinaryExpressions) {
        table1NonJoinBinaryExpressions = new ArrayList<>();

        if (nonBinaryExpressions.isANestedBinaryExpression()) {
            BinaryExpression leftNonJoinExp = (BinaryExpression) nonBinaryExpressions.getLeftOperand();
            FullyQualifiedAttributeIdentifier leftTempFQAttribute = (FullyQualifiedAttributeIdentifier) leftNonJoinExp.getLeftOperand();
            BinaryExpression rightNonJoinExp = (BinaryExpression) nonBinaryExpressions.getRightOperand();
            FullyQualifiedAttributeIdentifier rightTempFQAttribute = (FullyQualifiedAttributeIdentifier) rightNonJoinExp.getLeftOperand();

            nonJoinsOperator = nonBinaryExpressions.getOperator();

            if (nonJoinsOperator == BinaryExpression.Operator.AND) {
                if (leftTempFQAttribute.getIdentifier().equals(table1Identifier.getIdentifier())) {
                    table1NonJoinBinaryExpressions.add(leftNonJoinExp);
                } else {
                    binaryExpressionAttribute1 = leftTempFQAttribute;
                    // where is strict typed -> can use literal type
                    binaryExpressionAttributeType1 = leftNonJoinExp.getRightOperand() instanceof IntegerLiteral ? "INTEGER" : "VARCHAR";;;
                    binaryExpressionOperator1 = leftNonJoinExp.getOperator();
                    binaryExpressionConstant1 = leftNonJoinExp.getRightOperand().getIdentifier();
                }

                if (rightTempFQAttribute.getIdentifier().equals(table1Identifier.getIdentifier())) {
                    table1NonJoinBinaryExpressions.add(rightNonJoinExp);
                } else {
                    binaryExpressionAttribute2 = rightTempFQAttribute;
                    // where is strict typed -> can use literal type
                    binaryExpressionAttributeType2 = rightNonJoinExp.getRightOperand() instanceof IntegerLiteral ? "INTEGER" : "VARCHAR";;
                    binaryExpressionOperator2 = rightNonJoinExp.getOperator();
                    binaryExpressionConstant2 = rightNonJoinExp.getRightOperand().getIdentifier();
                }
            } else {
                binaryExpressionAttribute1 = leftTempFQAttribute;
                // where is strict typed -> can use literal type
                binaryExpressionAttributeType1 = leftNonJoinExp.getRightOperand() instanceof IntegerLiteral ? "INTEGER" : "VARCHAR";;;
                binaryExpressionOperator1 = leftNonJoinExp.getOperator();
                binaryExpressionConstant1 = leftNonJoinExp.getRightOperand().getIdentifier();

                binaryExpressionAttribute2 = rightTempFQAttribute;
                // where is strict typed -> can use literal type
                binaryExpressionAttributeType2 = rightNonJoinExp.getRightOperand() instanceof IntegerLiteral ? "INTEGER" : "VARCHAR";;
                binaryExpressionOperator2 = rightNonJoinExp.getOperator();
                binaryExpressionConstant2 = rightNonJoinExp.getRightOperand().getIdentifier();
            }
        } else {
            FullyQualifiedAttributeIdentifier tempFQAttribute = (FullyQualifiedAttributeIdentifier) nonBinaryExpressions.getLeftOperand();
            if (tempFQAttribute.getIdentifier().equals(table1Identifier.getIdentifier())) {
                table1NonJoinBinaryExpressions.add(nonBinaryExpressions); // condition can directly used to filter results
            } else {
                binaryExpressionAttribute1 = tempFQAttribute;
                // where is strict typed -> can use literal type
                binaryExpressionAttributeType1 = nonBinaryExpressions.getRightOperand() instanceof IntegerLiteral ? "INTEGER" : "VARCHAR";
                binaryExpressionOperator1 = nonBinaryExpressions.getOperator();
                binaryExpressionConstant1 = nonBinaryExpressions.getRightOperand().toString();
            }
        }
    }

    private void initializeAllRowAttributes() {
        rowFQAttributesForJoinedTable = new ArrayList<>();

        Table table1 = metadataManager.getTable(table1Identifier.getIdentifier());
        String table1Name = table1.getAttributeName();
        ArrayList<Column> dbColumns1 = table1.getColumns();
        for (Column c : dbColumns1) {
            String attribute = c.getAttributeName();
            rowFQAttributesForJoinedTable.add(new FullyQualifiedAttributeIdentifier(table1Name, attribute));
        }

        Table table2 = metadataManager.getTable(table2Identifier.getIdentifier());
        String table2Name = table2.getAttributeName();
        ArrayList<Column> dbColumns2 = table2.getColumns();
        for (Column c : dbColumns2) {
            String attribute = c.getAttributeName();
            rowFQAttributesForJoinedTable.add(new FullyQualifiedAttributeIdentifier(table2Name, attribute));
        }
    }

    private FedResultSet getFedResultSet(SelectNoGroupStatement noGroupStatement) throws FedException {
        FedResultSet fedResultSet;

        String specifictableName = noGroupStatement.getTableIdentifiers().get(0).getIdentifier();
        Table specifictable = metadataManager.getTable(specifictableName);

        if (noGroupStatement.getWhereClause() == null) {
            if (specifictable.getPartitionType() == PartitionType.None) {
                fedResultSet = new NoGroupDefaultResultSet(fedConnection, noGroupStatement);
            } else if (specifictable.getPartitionType() == PartitionType.Vertical) {
                fedResultSet = new NoGroupVerticalResultSet(fedConnection, noGroupStatement);
            } else {
                fedResultSet = new NoGroupHorizontalResultSet(fedConnection, noGroupStatement);
            }
        } else {
            BinaryExpression binaryExpression = noGroupStatement.getWhereClause().getBinaryExpression();
            if (binaryExpression.isANestedBinaryExpression()) {
                if (binaryExpression.getOperator() == BinaryExpression.Operator.AND) {
                    if (specifictable.getPartitionType() == PartitionType.None) {
                        fedResultSet = new NoGroupWithNonJoinAndNonJoinDefaultResultSet(fedConnection, noGroupStatement);
                    } else if (specifictable.getPartitionType() == PartitionType.Vertical) {
                        BinaryExpression leftBinaryExpression = (BinaryExpression) binaryExpression.getLeftOperand();
                        BinaryExpression righttBinaryExpression = (BinaryExpression) binaryExpression.getRightOperand();

                        int samseDB = getAttributesSameDB(specifictable,
                                ((FullyQualifiedAttributeIdentifier) leftBinaryExpression.getLeftOperand()).getAttributeIdentifier().getIdentifier(),
                                ((FullyQualifiedAttributeIdentifier) righttBinaryExpression.getLeftOperand()).getAttributeIdentifier().getIdentifier());

                        fedResultSet = new NoGroupWithNonJoinAndNonJoinVerticalResultSet(fedConnection, noGroupStatement, samseDB);
                    } else {
                        fedResultSet = new NoGroupWithNonJoinAndNonJoinHorizontalResultSet(fedConnection, noGroupStatement);
                    }
                } else {
                    if (specifictable.getPartitionType() == PartitionType.None) {
                        fedResultSet = new NoGroupWithNonJoinOrNonJoinDefaultResultSet(fedConnection, noGroupStatement);
                    } else if (specifictable.getPartitionType() == PartitionType.Vertical) {
                        fedResultSet = new NoGroupWithNonJoinOrNonJoinVerticalResultSet(fedConnection, noGroupStatement);
                    } else {
                        fedResultSet = new NoGroupWithNonJoinOrNonJoinHorizontalResultSet(fedConnection, noGroupStatement);
                    }
                }
            } else {
                if (specifictable.getPartitionType() == PartitionType.None) {
                    fedResultSet = new NoGroupWithNonJoinDefaultResultSet(fedConnection, noGroupStatement);
                } else if (specifictable.getPartitionType() == PartitionType.Vertical) {
                    fedResultSet = new NoGroupWithNonJoinVerticalResultSet(fedConnection, noGroupStatement);
                } else {
                    fedResultSet = new NoGroupWithNonJoinHorizontalResultSet(fedConnection, noGroupStatement);
                }
            }

        }

        return fedResultSet;
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

    private BinaryExpression.Operator getSwapBinaryExpressionOperator(BinaryExpression.Operator operator) throws FedException {
        BinaryExpression.Operator invertedOpertator;

        switch (operator) {
            case EQ:
                invertedOpertator = BinaryExpression.Operator.EQ;
                break;
            case LEQ:
                invertedOpertator = BinaryExpression.Operator.GEQ;
                break;
            default:
                throw new FedException(String.format("%s: Unexpected non join operator \"%s\"", this.getClass().getSimpleName(), operator.name()));
        }

        return invertedOpertator;
    }

    private boolean isFQAttributeRequestedInSelect(FullyQualifiedAttributeIdentifier fqAttribute, List<FullyQualifiedAttributeIdentifier> fqSelectAttributes) {
        boolean isRequested = false;

        for (FullyQualifiedAttributeIdentifier attribute : fqSelectAttributes) {
            if (attribute.getFullyQualifiedIdentifier().equals(fqAttribute.getFullyQualifiedIdentifier())) {
                isRequested = true;
                break;
            }
        }

        return isRequested;
    }

    private WhereClause getJoinWhere(String constant) {
        Literal constantLiteral;

        if (constant == null) {
            constantLiteral = new NullLiteral(constant);
        } else if (joinType.equalsIgnoreCase("INTEGER")) {
            constantLiteral = new IntegerLiteral(constant);
        } else {
            constantLiteral = new StringLiteral(constant);
        }

        BinaryExpression exp = new BinaryExpression(joinAttributeTable2, joinOperator, constantLiteral);
        return new WhereClause(exp);
    }

    private void intializeInternalRowAttributesForTable1() throws FedException {
        internalRowAttributesForTable1 = new ArrayList<>();

        for (int i = 1; i <= table1FedResultSet.getColumnCount(); i++) {
            internalRowAttributesForTable1.add(table1FedResultSet.getColumnName(i));
        }
    }

    private void intializeInternalRowAttributesForTable2() throws FedException {
        internalRowAttributesForTable2 = new ArrayList<>();

        for (int i = 1; i <= currentTable2FedResultSet.getColumnCount(); i++) {
            internalRowAttributesForTable2.add(currentTable2FedResultSet.getColumnName(i));
        }
    }

    private FedResultSetAccessInfo resolveColumnIndex(int index) throws FedException {
        FedResultSet fedresultSet;
        int resolvedColumnIndex;

        if (index > 0 && rowFQAttributesForJoinedTable.size() >= index) {
            FullyQualifiedAttributeIdentifier fqAttribute = rowFQAttributesForJoinedTable.get(index - 1);
            if (fqAttribute.getIdentifier().equals(table1Identifier.getIdentifier())) {
                fedresultSet = table1FedResultSet;
                resolvedColumnIndex = internalRowAttributesForTable1.indexOf(fqAttribute.getAttributeIdentifier().getIdentifier()) + 1;
            } else {
                resolvedColumnIndex = internalRowAttributesForTable2.indexOf(fqAttribute.getAttributeIdentifier().getIdentifier()) + 1;
                fedresultSet = currentTable2FedResultSet;
            }
        } else {
            throw new FedException(String.format("%s: Invalid column index.", this.getClass().getSimpleName()));
        }

        return new FedResultSetAccessInfo(fedresultSet, resolvedColumnIndex);
    }

    public void forcecurrentTable2FedResultSetInitialization() throws FedException {
        currentTable2FedResultSet = getFedResultSet(table2NoGroupStatement);
        intializeInternalRowAttributesForTable2();
    }

}
