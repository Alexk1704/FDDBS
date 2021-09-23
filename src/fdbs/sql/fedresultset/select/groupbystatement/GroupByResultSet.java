package fdbs.sql.fedresultset.select.groupbystatement;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.NoGroupDefaultResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.NoGroupHorizontalResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.NoGroupVerticalResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.join.NoGroupWithJoinResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin.NoGroupWithNonJoinDefaultResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin.NoGroupWithNonJoinHorizontalResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin.NoGroupWithNonJoinVerticalResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinandnonjoin.NoGroupWithNonJoinAndNonJoinDefaultResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinandnonjoin.NoGroupWithNonJoinAndNonJoinHorizontalResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinandnonjoin.NoGroupWithNonJoinAndNonJoinVerticalResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinornonjoin.NoGroupWithNonJoinOrNonJoinDefaultResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinornonjoin.NoGroupWithNonJoinOrNonJoinHorizontalResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinornonjoin.NoGroupWithNonJoinOrNonJoinVerticalResultSet;
import fdbs.sql.meta.PartitionType;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.clause.HavingClause;
import fdbs.sql.parser.ast.clause.WhereClause;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.expression.BinaryExpression.Operator;
import fdbs.sql.parser.ast.function.CountFunction;
import fdbs.sql.parser.ast.function.Function;
import fdbs.sql.parser.ast.function.MaxFunction;
import fdbs.sql.parser.ast.function.SumFunction;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.identifier.TableIdentifier;
import fdbs.sql.parser.ast.statement.select.SelectGroupStatement;
import fdbs.sql.parser.ast.statement.select.SelectNoGroupStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class GroupByResultSet extends FedResultSet {

    private SelectGroupStatement selectGroupStatement;
    private FedResultSet fedResultSet;

    private List<FullyQualifiedAttributeIdentifier> fqaiList = new ArrayList<>();
    private String groupByAttributeName = null;
    private String aggregationAttributeName = null;
    private String aggregationAttributeTableName = null;
    private String groupByAttributeTableName = null;
    private String groupByAttributeType = null;
    private String functionType = null;
    private Operator havingOperator = null;
    private int havingComparisonValue;
    private HashMap<String, GroupResult> groupResultMap = null;
    private ArrayList<String> groupResultMapping = new ArrayList<>();
    private int rowCounter = 0;

    public GroupByResultSet(FedConnection fedConnection, SelectGroupStatement statement) throws FedException {
        super(fedConnection, statement);
        try {
            selectGroupStatement = statement;
            resolveStatement();
        } catch (FedException ex) {
            closeLocalResultSets();
            throw ex;
        }
    }

    @Override

    protected boolean privateNext() throws Exception {
        boolean next = false;
        if (groupResultMapping.isEmpty() || rowCounter >= groupResultMapping.size()) {
            next = false;
        } else {
            next = true;
            rowCounter++;
        }
        return next;
    }

    @Override
    protected String getPrivateString(int columnIndex) throws Exception {
        int currentRow = rowCounter - 1;
        String stringValue = "";
        switch (functionType) {
            case "COUNT":
                switch (columnIndex) {
                    case 1:
                        stringValue = groupResultMapping.get(currentRow);
                        break;
                    case 2:
                        GroupResult groupResult = groupResultMap.get(groupResultMapping.get(currentRow));
                        stringValue = Integer.toString(groupResult.getCount());
                        break;
                    default:
                        throw new FedException(String.format("%s: Requested an invalid column index", this.getClass().getSimpleName()));
                }
                break;
            case "SUM":
                switch (columnIndex) {
                    case 1:
                        stringValue = groupResultMapping.get(currentRow);
                        break;
                    case 2:
                        GroupResult groupResult = groupResultMap.get(groupResultMapping.get(currentRow));
                        stringValue = Integer.toString(groupResult.getSum());
                        break;
                    default:
                        throw new FedException(String.format("%s: Requested an invalid column index", this.getClass().getSimpleName()));
                }
                break;
            case "MAX":
                switch (columnIndex) {
                    case 1:
                        stringValue = groupResultMapping.get(currentRow);
                        break;
                    case 2:
                        GroupResult groupResult = groupResultMap.get(groupResultMapping.get(currentRow));
                        stringValue = Integer.toString(groupResult.getMax());
                        break;
                    default:
                        throw new FedException(String.format("%s: Requested an invalid column index", this.getClass().getSimpleName()));
                }
                break;
            default:
                throw new FedException(String.format("%s: Unexpected function type, expected: COUNT, SUM or MAX.", this.getClass().getSimpleName()));
        }
        return stringValue;
    }

    @Override
    protected int getPrivateInt(int columnIndex) throws Exception {
        int currentRow = rowCounter - 1;
        int intValue = 0;
        switch (functionType) {
            case "COUNT":
                switch (columnIndex) {
                    case 1:
                        intValue = Integer.parseInt(groupResultMapping.get(currentRow));
                        break;
                    case 2:
                        GroupResult groupResult = groupResultMap.get(groupResultMapping.get(currentRow));
                        intValue = groupResult.getCount();
                        break;
                    default:
                        throw new FedException(String.format("%s: Requested an invalid column index", this.getClass().getSimpleName()));
                }
                break;
            case "SUM":
                switch (columnIndex) {
                    case 1:
                        intValue = Integer.parseInt(groupResultMapping.get(currentRow));
                        break;
                    case 2:
                        GroupResult groupResult = groupResultMap.get(groupResultMapping.get(currentRow));
                        intValue = groupResult.getSum();
                        break;
                    default:
                        throw new FedException(String.format("%s: Requested an invalid column index", this.getClass().getSimpleName()));
                }
                break;
            case "MAX":
                switch (columnIndex) {
                    case 1:
                        intValue = Integer.parseInt(groupResultMapping.get(currentRow));
                        break;
                    case 2:
                        GroupResult groupResult = groupResultMap.get(groupResultMapping.get(currentRow));
                        intValue = groupResult.getMax();
                        break;
                    default:
                        throw new FedException(String.format("%s: Requested an invalid column index", this.getClass().getSimpleName()));
                }
                break;
            default:
                throw new FedException(String.format("%s: Unexpected function type, expected: COUNT, SUM or MAX.", this.getClass().getSimpleName()));
        }
        return intValue;
    }

    @Override
    protected int getPrivateColumnCount() throws Exception {
        return 2;
    }

    @Override
    protected String getPrivateColumnName(int columnIndex) throws Exception {
        String columnName = null;
        switch (columnIndex) {
            case 1:
                columnName = groupByAttributeName;
                break;
            case 2:
                switch (functionType) {
                    case "COUNT":
                        columnName = "COUNT(*)";
                        break;
                    case "SUM":
                        columnName = String.format("SUM(%s.%s)", aggregationAttributeTableName, aggregationAttributeName);
                        break;
                    case "MAX":
                        columnName = String.format("MAX(%s.%s)", aggregationAttributeTableName, aggregationAttributeName);
                        break;
                    default:
                        throw new FedException(String.format("%s: Unexpected function type, expected: COUNT, SUM or MAX.", this.getClass().getSimpleName()));
                }
                break;
            default:
                throw new FedException(String.format("%s: Requested an invalid column index", this.getClass().getSimpleName()));
        }
        return columnName;
    }

    @Override
    protected int getPrivateColumnType(int columnIndex) throws Exception {
        int columnType = 0;
        switch (columnIndex) {
            case 1:
                if (groupByAttributeType.equals("INTEGER")) {
                    if ((functionType.equals("SUM") || functionType.equals("MAX")) && groupResultMapping.isEmpty()) {
                        columnType = 2;
                    } else {
                        columnType = 4;
                    }
                } else {
                    columnType = 12;
                }
                break;
            case 2:
                columnType = 2;
                break;
            default:
                throw new FedException(String.format("%s: Requested an invalid column index", this.getClass().getSimpleName()));
        }
        return columnType;
    }

    @Override
    protected FedException closeLocalResultSets() {
        FedException exception = null;
        if (fedResultSet != null && fedResultSet.isClosed() == false) {
            try {
                fedResultSet.close();
            } catch (FedException ex) {
                exception = ex;
            }
        }

        fqaiList = null;
        groupByAttributeName = null;
        aggregationAttributeName = null;
        aggregationAttributeTableName = null;
        groupByAttributeTableName = null;
        groupByAttributeType = null;
        functionType = null;
        groupResultMap = null;
        groupResultMapping.clear();
        rowCounter = 0;

        return exception;
    }

    @Override
    protected void resolveStatement() throws FedException {

        FullyQualifiedAttributeIdentifier groupStatementAttribute = selectGroupStatement.getFQAttributeIdentifier();
        fqaiList.add(groupStatementAttribute);
        groupByAttributeName = groupStatementAttribute.getAttributeIdentifier().getIdentifier();
        groupByAttributeTableName = groupStatementAttribute.getIdentifier();

        Function function = selectGroupStatement.getFunction();
        if (function instanceof CountFunction) {
            functionType = "COUNT";
        } else if (function instanceof MaxFunction) {
            functionType = "MAX";
            MaxFunction maxFunction = (MaxFunction) function;
            FullyQualifiedAttributeIdentifier maxFunctionAttribute = maxFunction.getFQAttributeIdentifier();
            fqaiList.add(maxFunctionAttribute);
            aggregationAttributeName = maxFunctionAttribute.getAttributeIdentifier().getIdentifier();
            aggregationAttributeTableName = maxFunctionAttribute.getIdentifier();
        } else if (function instanceof SumFunction) {
            functionType = "SUM";
            SumFunction sumFunction = (SumFunction) function;
            FullyQualifiedAttributeIdentifier sumFunctionAttribute = sumFunction.getFQAttributeIdentifier();
            fqaiList.add(sumFunctionAttribute);
            aggregationAttributeName = sumFunctionAttribute.getAttributeIdentifier().getIdentifier();
            aggregationAttributeTableName = sumFunctionAttribute.getIdentifier();
        }

        if (hasHavingClause()) {
            processHavingClause();
        }

        List<TableIdentifier> tableIdentifiers = selectGroupStatement.getTableIdentifiers();
        WhereClause whereClause = selectGroupStatement.getWhereClause();
        SelectNoGroupStatement noGroupStatement = new SelectNoGroupStatement(fqaiList, tableIdentifiers, whereClause);

        if (whereClause == null) {
            fedResultSet = getFedResultSet(noGroupStatement, null);
        } else {
            BinaryExpression whereExpression = whereClause.getBinaryExpression();
            fedResultSet = getFedResultSet(noGroupStatement, whereExpression);
        }
        groupResultMap = initializeGroupedResults(fedResultSet);
    }

    private FedResultSet getFedResultSet(SelectNoGroupStatement noGroupStatement, BinaryExpression whereExpression) throws FedException {

        String specificTableName = noGroupStatement.getTableIdentifiers().get(0).getIdentifier();
        Table specificTable = metadataManager.getTable(specificTableName);
        PartitionType tablePartitionType = specificTable.getPartitionType();

        if (noGroupStatement.getWhereClause() == null) {
            if (tablePartitionType == null) {
                throw new FedException(String.format("%s: Table has no valid partitioning type", this.getClass().getSimpleName()));
            } else {
                switch (tablePartitionType) {
                    case None:
                        fedResultSet = new NoGroupDefaultResultSet(fedConnection, noGroupStatement);
                        break;
                    case Vertical:
                        fedResultSet = new NoGroupVerticalResultSet(fedConnection, noGroupStatement);
                        break;
                    case Horizontal:
                        fedResultSet = new NoGroupHorizontalResultSet(fedConnection, noGroupStatement);
                        break;
                    default:
                        throw new FedException(String.format("%s: Unexpected table partitioning type", this.getClass().getSimpleName()));
                }
            }
        } else {
            switch (getAmountOfNestedWheres(whereExpression)) {
                case 1:
                    if (whereExpression.isJoinCondition()) { // JOIN
                        fedResultSet = new NoGroupWithJoinResultSet(fedConnection, noGroupStatement);
                    } else {
                        if (tablePartitionType == null) {
                            throw new FedException(String.format("%s: Table has no valid partitioning type", this.getClass().getSimpleName()));
                        } else {
                            switch (tablePartitionType) {
                                case None:
                                    fedResultSet = new NoGroupWithNonJoinDefaultResultSet(fedConnection, noGroupStatement);
                                    break;
                                case Vertical:
                                    fedResultSet = new NoGroupWithNonJoinVerticalResultSet(fedConnection, noGroupStatement);
                                    break;
                                case Horizontal:
                                    fedResultSet = new NoGroupWithNonJoinHorizontalResultSet(fedConnection, noGroupStatement);
                                    break;
                                default:
                                    throw new FedException(String.format("%s: Unexpected table partitioning type", this.getClass().getSimpleName()));
                            }
                        }
                    }
                    break;
                case 2:
                    Operator binaryExpressionOperator = whereExpression.getOperator();
                    BinaryExpression leftBinaryExpression = (BinaryExpression) whereExpression.getLeftOperand();
                    if (leftBinaryExpression.isJoinCondition()) {
                        fedResultSet = new NoGroupWithJoinResultSet(fedConnection, noGroupStatement);
                    } else {

                        switch (binaryExpressionOperator) {
                            case AND:
                                if (tablePartitionType == null) {
                                    throw new FedException(String.format("%s: Table has no valid partitioning type", this.getClass().getSimpleName()));
                                } else {
                                    switch (tablePartitionType) {
                                        case None:
                                            fedResultSet = new NoGroupWithNonJoinAndNonJoinDefaultResultSet(fedConnection, noGroupStatement);
                                            break;
                                        case Vertical:
                                            BinaryExpression rightBinaryExpression = (BinaryExpression) whereExpression.getRightOperand();
                                            FullyQualifiedAttributeIdentifier leftBinaryExpressionLeftAttribute = (FullyQualifiedAttributeIdentifier) leftBinaryExpression.getLeftOperand();
                                            FullyQualifiedAttributeIdentifier rightBinaryExpressionLeftAttribute = (FullyQualifiedAttributeIdentifier) rightBinaryExpression.getLeftOperand();
                                            List<Integer> databasesForColumn1 = specificTable.getDatabasesForColumn(leftBinaryExpressionLeftAttribute.getAttributeIdentifier().getIdentifier());
                                            List<Integer> databasesForColumn2 = specificTable.getDatabasesForColumn(rightBinaryExpressionLeftAttribute.getAttributeIdentifier().getIdentifier());
                                            int sameDB = 0;
                                            for (int dbId : databasesForColumn1) {
                                                if (databasesForColumn2.contains(dbId)) {
                                                    sameDB = dbId;
                                                }
                                            }
                                            fedResultSet = new NoGroupWithNonJoinAndNonJoinVerticalResultSet(fedConnection, noGroupStatement, sameDB);
                                            break;
                                        case Horizontal:
                                            fedResultSet = new NoGroupWithNonJoinAndNonJoinHorizontalResultSet(fedConnection, noGroupStatement);
                                            break;
                                        default:
                                            throw new FedException(String.format("%s: Unexpected table partitioning type", this.getClass().getSimpleName()));
                                    }
                                }
                                break;
                            case OR:
                                if (tablePartitionType == null) {
                                    throw new FedException(String.format("%s: Table has no valid partitioning type", this.getClass().getSimpleName()));
                                } else {
                                    switch (tablePartitionType) {
                                        case None:
                                            fedResultSet = new NoGroupWithNonJoinOrNonJoinDefaultResultSet(fedConnection, noGroupStatement);
                                            break;
                                        case Vertical:
                                            fedResultSet = new NoGroupWithNonJoinOrNonJoinVerticalResultSet(fedConnection, noGroupStatement);
                                            break;
                                        case Horizontal:
                                            fedResultSet = new NoGroupWithNonJoinOrNonJoinHorizontalResultSet(fedConnection, noGroupStatement);
                                            break;
                                        default:
                                            throw new FedException(String.format("%s: Unexpected table partitioning type", this.getClass().getSimpleName()));
                                    }
                                }
                                break;
                            default:
                                throw new FedException(String.format("%s: Unexpected non join operator \"%s\"", this.getClass().getSimpleName(), binaryExpressionOperator.name()));
                        }
                    }
                    break;
                case 3:
                    fedResultSet = new NoGroupWithJoinResultSet(fedConnection, noGroupStatement);
                    break;
                default:
                    throw new FedException(String.format("%s: Unexpected binary expression level", this.getClass().getSimpleName()));
            }
        }
        return fedResultSet;
    }

    private boolean hasHavingClause() {
        return selectGroupStatement.getHavingClause() != null;
    }

    private void processHavingClause() {
        HavingClause havingClause = selectGroupStatement.getHavingClause();
        BinaryExpression havingBinaryExpression = havingClause.getCountComparisonExpression();
        havingComparisonValue = Integer.parseInt(havingBinaryExpression.getRightOperand().getIdentifier());
        havingOperator = havingBinaryExpression.getOperator();
    }

    private boolean checkHavingClause(int count) throws FedException {
        boolean isValid = false;
        switch (havingOperator) {
            case LEQ:
                isValid = count <= havingComparisonValue;
                break;
            case EQ:
                isValid = count == havingComparisonValue;
                break;
            case GEQ:
                isValid = count >= havingComparisonValue;
                break;
            case NEQ:
                isValid = count != havingComparisonValue;
                break;
            case LT:
                isValid = count < havingComparisonValue;
                break;
            case GT:
                isValid = count > havingComparisonValue;
                break;
            default:
                throw new FedException(String.format("%s: Unexpected operator for a binary 'having' expression", this.getClass().getSimpleName()));
        }
        return isValid;
    }

    private HashMap<String, GroupResult> initializeGroupedResults(FedResultSet fedResultSet) throws FedException {
        HashMap<String, GroupResult> groupedResultsMap = new HashMap<>();

        groupByAttributeType = metadataManager.getColumnType(groupByAttributeTableName, groupByAttributeName);

        while (fedResultSet.next()) {
            String groupValue = fedResultSet.getString(1);
            switch (functionType) {
                case "COUNT":
                    updateCountGroupResults(groupedResultsMap, groupValue);
                    break;
                case "SUM":
                    int aggregationValueSum = fedResultSet.getInt(2);
                    updateSumGroupResults(groupedResultsMap, groupValue, aggregationValueSum, hasHavingClause());
                    break;
                case "MAX":
                    int aggregationValueMax = fedResultSet.getInt(2);
                    updateMaxGroupResults(groupedResultsMap, groupValue, aggregationValueMax, hasHavingClause());
                    break;
                default:
                    throw new FedException(String.format("%s: Unexpected function type, expected: COUNT, SUM or MAX.", this.getClass().getSimpleName()));
            }
        }
        fedResultSet.close();

        if (hasHavingClause()) {
            clearMapForHavingClause(groupedResultsMap);
        }

        return groupedResultsMap;
    }

    private void clearMapForHavingClause(HashMap<String, GroupResult> groupedResultsMap) throws FedException {
        ArrayList<String> itemsToRemove = new ArrayList<>();
        for (Map.Entry<String, GroupResult> entry : groupedResultsMap.entrySet()) {
            String groupAttribute = entry.getKey();
            GroupResult aggregationObject = entry.getValue();
            int countValue = aggregationObject.getCount();
            if (!checkHavingClause(countValue)) {
                itemsToRemove.add(groupAttribute);
            }
        }
        for (String items : itemsToRemove) {
            if (groupedResultsMap.containsKey(items)) {
                groupedResultsMap.remove(items);
                groupResultMapping.remove(items);
            }
        }
    }

    private void updateMaxGroupResults(HashMap<String, GroupResult> groupedResultsMap, String groupValue, int aggregationValue, boolean hasHaving) {
        if (!groupedResultsMap.containsKey(groupValue) && !groupResultMapping.contains(groupValue)) {
            GroupResult groupResult = new GroupResult();
            if (hasHaving) {
                groupResult.setCount(1);
            }
            groupResult.setMax(aggregationValue);
            groupedResultsMap.put(groupValue, groupResult);
            groupResultMapping.add(groupValue);
        } else {
            GroupResult groupResult = groupedResultsMap.get(groupValue);
            if (hasHaving) {
                int newCount = groupResult.getCount() + 1;
                groupResult.setCount(newCount);
            }
            int currentMax = groupResult.getMax();
            if (aggregationValue > currentMax) {
                groupResult.setMax(aggregationValue);
                groupedResultsMap.put(groupValue, groupResult);
            }
        }
    }

    private void updateSumGroupResults(HashMap<String, GroupResult> groupedResultsMap, String groupValue, int aggregationValue, boolean hasHaving) {
        if (!groupedResultsMap.containsKey(groupValue) && !groupResultMapping.contains(groupValue)) {
            GroupResult groupResult = new GroupResult();
            if (hasHaving) {
                groupResult.setCount(1);
            }
            groupResult.setSum(aggregationValue);
            groupedResultsMap.put(groupValue, groupResult);
            groupResultMapping.add(groupValue);
        } else {
            GroupResult groupResult = groupedResultsMap.get(groupValue);
            if (hasHaving) {
                int newCount = groupResult.getCount() + 1;
                groupResult.setCount(newCount);
            }
            int newSum = groupResult.getSum() + aggregationValue;
            groupResult.setSum(newSum);
            groupedResultsMap.put(groupValue, groupResult);
        }
    }

    private void updateCountGroupResults(HashMap<String, GroupResult> groupedResultsMap, String groupValue) {
        if (!groupedResultsMap.containsKey(groupValue) && !groupResultMapping.contains(groupValue)) {
            GroupResult groupResult = new GroupResult();
            groupResult.setCount(1);
            groupedResultsMap.put(groupValue, groupResult);
            groupResultMapping.add(groupValue);
        } else {
            GroupResult groupResult = groupedResultsMap.get(groupValue);
            int newCount = groupResult.getCount() + 1;
            groupResult.setCount(newCount);
            groupedResultsMap.put(groupValue, groupResult);
        }
    }
}
