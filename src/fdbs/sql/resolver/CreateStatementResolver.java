package fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.BetweenConstraint;
import fdbs.sql.meta.BinaryConstraint;
import fdbs.sql.meta.BinaryType;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.Constraint;
import fdbs.sql.meta.ConstraintType;
import static fdbs.sql.meta.ConstraintType.CheckBetween;
import static fdbs.sql.meta.ConstraintType.CheckComparison;
import static fdbs.sql.meta.ConstraintType.CheckNull;
import static fdbs.sql.meta.ConstraintType.ForeignKey;
import static fdbs.sql.meta.ConstraintType.PrimaryKey;
import static fdbs.sql.meta.ConstraintType.Unique;
import fdbs.sql.meta.HorizontalPartition;
import fdbs.sql.meta.PartitionType;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.statement.CreateTableStatement;
import fdbs.util.logger.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory sub-class which resolves the statement from the AST node and creates
 * a specific ResolvedStatement object.
 */
public class CreateStatementResolver extends StatementResolverManager {

    private int verticalPartitions;
    private int horizontalPartitions;
    private boolean pkCanAdded;
    private boolean pkAttrAdded;

    private ArrayList<String> executableStatementsListDB1 = new ArrayList<>();
    private ArrayList<String> executableStatementsListDB2 = new ArrayList<>();
    private ArrayList<String> executableStatementsListDB3 = new ArrayList<>();

    private HashMap<Integer, LinkedHashMap<String, Column>> dbRelatedColumns;
    private HashMap<String, Column> columnsByName;
    private ArrayList<HorizontalPartition> horizontalPartition;

    /**
     * Constructor for the specific resolver
     *
     * @param fedConnection
     * @throws FedException
     */
    public CreateStatementResolver(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    /**
     * This method creates a ResolvedStatement object and therefor runs all the
     * necessary methods to prepare the ASTnode statement and convert it into a
     * data structure containing executable sub-statements for all targeted
     * databases to pass to the executer module.
     *
     * @param tree, AST
     * @return resolvedStatement, ResolvedStatement object
     * @throws fdbs.sql.FedException
     */
    @Override
    public SQLExecuterTask resolveStatement(AST tree) throws FedException {
        SQLExecuterTask executerTask;
        metadataManager.handleQuery(tree);

        try {
            executerTask = createStatement(tree);
        } catch (FedException ex) {
            throw new FedException(String.format("%s: An error occurred while resolving create statement.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return executerTask;
    }

    /**
     * This method calls the specific methods that are needed to process the
     * global statements, after all constraints were processed and the
     * partitions were set up this method passes the sub statements to a
     * SQLExecuterTask object and adds the sub tasks.
     *
     * @return SQLExecuterTask
     * @throws FedException
     */
    private SQLExecuterTask createStatement(AST tree) throws FedException {
        CreateTableStatement ctStatement = (CreateTableStatement) tree.getRoot();
        String tableName = ctStatement.getTableIdentifier().getIdentifier();
        ArrayList<Column> columnList = metadataManager.getColumns(tableName);
        ArrayList<Constraint> constraintList = metadataManager.getConstraintsByTable(tableName);

        try {
            if (isVerticallyPartitioned(tableName)) {
                dbRelatedColumns = metadataManager.getTable(tableName).getDatabaseRelatedColumns();
                verticalPartitions = 0;
                pkCanAdded = false;
                pkAttrAdded = false;
                processVerticalPartitioning(dbRelatedColumns);
            } else if (isHorizontallyPartitioned(tableName)) {
                horizontalPartition = metadataManager.getHorizontalPartition(tableName);
                horizontalPartitions = 0;
                processHorizontalPartitioning(horizontalPartition, columnList);
            } else {
                processDefaultPartitioning(columnList);
            }
            columnsByName = metadataManager.getTable(tableName).getColumnsByName();
            processConstraints(constraintList, tableName, columnsByName);
        } catch (StatementResolverException ex) {
            throw new FedException(String.format("%s: Something went wrong while creating the statement.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        SQLExecuterTask task = new SQLExecuterTask();
        Logger.infoln("Start to test select count all table statement result sets.");
        if (verticalPartitions == 0 && horizontalPartitions == 0) {
            String stmntResult = String.join(", ", executableStatementsListDB1);
            String createTableString;
            createTableString = String.format("CREATE TABLE %s ( %s )", tableName, stmntResult);
            task.addSubTask(createTableString, true, 1);
            Logger.infoln(String.format("%s: Added following statement as sub-task to DB1:%s", this.getClass().getSimpleName(), createTableString));
        }
        if (verticalPartitions == 2 || horizontalPartitions == 2) {
            String stmntResultDb1 = String.join(", ", executableStatementsListDB1);
            String createTableStringDb1;
            createTableStringDb1 = String.format("CREATE TABLE %s ( %s )", tableName, stmntResultDb1);
            task.addSubTask(createTableStringDb1, true, 1);
            Logger.infoln(String.format("%s: Added following statement as sub-task to DB1:%s", this.getClass().getSimpleName(), createTableStringDb1));

            String stmntResultDb2 = String.join(", ", executableStatementsListDB2);
            String createTableStringDb2;
            createTableStringDb2 = String.format("CREATE TABLE %s ( %s )", tableName, stmntResultDb2);
            task.addSubTask(createTableStringDb2, true, 2);
            Logger.infoln(String.format("%s: Added following statement as sub-task to DB2:%s", this.getClass().getSimpleName(), createTableStringDb2));
        }
        if (verticalPartitions == 3 || horizontalPartitions == 3) {
            String stmntResultDb1 = String.join(", ", executableStatementsListDB1);
            String createTableStringDb1;
            createTableStringDb1 = String.format("CREATE TABLE %s ( %s )", tableName, stmntResultDb1);
            task.addSubTask(createTableStringDb1, true, 1);
            Logger.infoln(String.format("%s: Added following statement as sub-task to DB1:%s", this.getClass().getSimpleName(), createTableStringDb1));

            String stmntResultDb2 = String.join(", ", executableStatementsListDB2);
            String createTableStringDb2;
            createTableStringDb2 = String.format("CREATE TABLE %s ( %s )", tableName, stmntResultDb2);
            task.addSubTask(createTableStringDb2, true, 2);
            Logger.infoln(String.format("%s: Added following statement as sub-task to DB2:%s", this.getClass().getSimpleName(), createTableStringDb2));

            String stmntResultDb3 = String.join(", ", executableStatementsListDB3);
            String createTableStringDb3;
            createTableStringDb3 = String.format("CREATE TABLE %s ( %s )", tableName, stmntResultDb3);
            task.addSubTask(createTableStringDb3, true, 3);
            Logger.infoln(String.format("%s: Added following statement as sub-task to DB3:%s", this.getClass().getSimpleName(), createTableStringDb3));
        }
        return task;
    }

    /**
     * Table has to be vertical partitioned, this method processes and saves the
     * attributes for each sub database in separate array lists.
     *
     * @param dbRelatedColumns
     * @throws FedException
     */
    private void processVerticalPartitioning(HashMap<Integer, LinkedHashMap<String, Column>> dbRelatedColumns) throws StatementResolverException {
        if (dbRelatedColumns == null || dbRelatedColumns.isEmpty()) {
            throw new StatementResolverException(String.format("%s: Vertical partitioning does not exist for this table.", this.getClass().getSimpleName()));
        } else {
            Iterator<Map.Entry<Integer, LinkedHashMap<String, Column>>> parent = dbRelatedColumns.entrySet().iterator();
            while (parent.hasNext()) {
                Map.Entry<Integer, LinkedHashMap<String, Column>> parentPair = parent.next();
                Iterator<Map.Entry<String, Column>> child = (parentPair.getValue()).entrySet().iterator();
                if (parentPair.getKey() == 1) {
                    if (verticalPartitions == 0) {
                        verticalPartitions = 2;
                    }
                    while (child.hasNext()) {
                        Map.Entry childPair = child.next();
                        String attributeName = (String) childPair.getKey();
                        Column column = (Column) childPair.getValue();
                        String attributeType = column.getType();
                        String attributeDefaultValue = column.getDefaultValue();
                        String attributeStatement = attributeName + " " + attributeType;
                        if (attributeDefaultValue != null) {
                            if (containsIgnoreCase(attributeType, "int")) {
                                attributeStatement = attributeStatement.concat(" DEFAULT " + attributeDefaultValue);
                            } else {
                                attributeStatement = attributeStatement.concat(" DEFAULT " + "'" + attributeDefaultValue + "'");
                            }
                        }
                        executableStatementsListDB1.add(attributeStatement);
                    }
                } else if (parentPair.getKey() == 2) {
                    if (verticalPartitions == 0) {
                        verticalPartitions = 2;
                    }
                    while (child.hasNext()) {
                        Map.Entry childPair = child.next();
                        String attributeName = (String) childPair.getKey();
                        Column column = (Column) childPair.getValue();
                        String attributeType = column.getType();
                        String attributeDefaultValue = column.getDefaultValue();
                        String attributeStatement = attributeName + " " + attributeType;
                        if (attributeDefaultValue != null) {
                            if (containsIgnoreCase(attributeType, "int")) {
                                attributeStatement = attributeStatement.concat(" DEFAULT " + attributeDefaultValue);
                            } else {
                                attributeStatement = attributeStatement.concat(" DEFAULT " + "'" + attributeDefaultValue + "'");
                            }
                        }
                        executableStatementsListDB2.add(attributeStatement);
                    }
                }
                if (parentPair.getKey() == 3 && !parentPair.getValue().isEmpty()) {
                    if (verticalPartitions == 0 || verticalPartitions == 2) {
                        verticalPartitions = 3;
                    }
                    while (child.hasNext()) {
                        Map.Entry childPair = child.next();
                        String attributeName = (String) childPair.getKey();
                        Column column = (Column) childPair.getValue();
                        String attributeType = column.getType();
                        String attributeDefaultValue = column.getDefaultValue();
                        String attributeStatement = attributeName + " " + attributeType;
                        if (attributeDefaultValue != null) {
                            if (containsIgnoreCase(attributeType, "int")) {
                                attributeStatement = attributeStatement.concat(" DEFAULT " + attributeDefaultValue);
                            } else {
                                attributeStatement = attributeStatement.concat(" DEFAULT " + "'" + attributeDefaultValue + "'");
                            }
                        }
                        executableStatementsListDB3.add(attributeStatement);
                    }
                }
            }
        }
    }

    /**
     * Horizontal partitioning was detected, in case there is only one boundary:
     * Every attribute gets saved on DB1 & DB2. In case there are two
     * boundaries: Every attribute gets saved on DB1, DB2, DB3
     *
     * @param horizontalPartition
     * @param columnList
     * @throws StatementResolverException
     */
    private void processHorizontalPartitioning(ArrayList<HorizontalPartition> horizontalPartition,
            ArrayList<Column> columnList) throws StatementResolverException {
        if (horizontalPartition == null || horizontalPartition.isEmpty()) {
            throw new StatementResolverException(String.format("%s: Horizontal partitioning does not exist for this fed-table.", this.getClass().getSimpleName()));
        } else if (columnList == null || columnList.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No column list available for this fed-table.", this.getClass().getSimpleName()));
        } else {
            if (horizontalPartition.size() == 2) {
                horizontalPartitions = 2;
                for (int i = 0; i < columnList.size(); i++) {
                    Column column = columnList.get(i);
                    String attributeName = column.getAttributeName();
                    String attributeType = column.getType();
                    String attributeDefaultValue = column.getDefaultValue();
                    String attributeStatement = attributeName + " " + attributeType;
                    if (attributeDefaultValue != null) {
                        if (containsIgnoreCase(attributeType, "int")) {
                            attributeStatement = attributeStatement.concat(" DEFAULT " + attributeDefaultValue);
                        } else {
                            attributeStatement = attributeStatement.concat(" DEFAULT " + "'" + attributeDefaultValue + "'");
                        }
                    }
                    addStatementsToTwoLists(attributeStatement, 1, 2);
                }
            }
            if (horizontalPartition.size() == 3) {
                horizontalPartitions = 3;
                for (int i = 0; i < columnList.size(); i++) {
                    Column column = columnList.get(i);
                    String attributeName = column.getAttributeName();
                    String attributeType = column.getType();
                    String attributeDefaultValue = column.getDefaultValue();
                    String attributeStatement = attributeName + " " + attributeType;
                    if (attributeDefaultValue != null) {
                        if (containsIgnoreCase(attributeType, "int")) {
                            attributeStatement = attributeStatement.concat(" DEFAULT " + attributeDefaultValue);
                        } else {
                            attributeStatement = attributeStatement.concat(" DEFAULT " + "'" + attributeDefaultValue + "'");
                        }
                    }
                    addStatementsToAllLists(attributeStatement);
                }
            }
        }
    }

    /**
     * No vertical nor horizontal partitioning has to be done, so the default
     * mode saves all the attributes on DB1.
     *
     * @param columnList
     * @throws StatementResolverException
     */
    private void processDefaultPartitioning(ArrayList<Column> columnList) throws StatementResolverException {
        if (columnList == null || columnList.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No column list available for this fed-table.", this.getClass().getSimpleName()));
        } else {
            for (int i = 0; i < columnList.size(); i++) {
                Column column = columnList.get(i);
                String attributeName = column.getAttributeName();
                String attributeType = column.getType();
                String attributeDefaultValue = column.getDefaultValue();
                String attributeStatement = attributeName + " " + attributeType;
                if (attributeDefaultValue != null) {
                    if (containsIgnoreCase(attributeType, "int")) {
                        attributeStatement = attributeStatement.concat(" DEFAULT " + attributeDefaultValue);
                    } else {
                        attributeStatement = attributeStatement.concat(" DEFAULT " + "'" + attributeDefaultValue + "'");
                    }
                }
                executableStatementsListDB1.add(attributeStatement);
            }
        }
    }

    /**
     * Iterates through the constraint list and calls the specific methods to
     * create the constraint statements for each targeted sub-database.
     *
     * @param constraintList
     * @param tableName
     * @param verticalPartition
     * @throws StatementResolverException
     */
    private void processConstraints(ArrayList<Constraint> constraintList, String tableName,
            HashMap<String, Column> columnsByName) throws StatementResolverException {
        if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else if (constraintList == null || constraintList.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No constraint list available for this fed-table.", this.getClass().getSimpleName()));
        } else {
            for (int i = 0; i < constraintList.size(); i++) {
                Constraint constraint = constraintList.get(i);
                ConstraintType constraintType = constraint.getType();
                if (null != constraintType) {
                    switch (constraintType) {
                        case PrimaryKey:
                            processPrimaryKeyConstraint(constraint, tableName, columnsByName);
                            break;
                        case ForeignKey:
                            processForeignKeyConstraint(constraint, tableName);
                            break;
                        case CheckComparison:
                            processCheckComparisonConstraint(constraint, tableName);
                            break;
                        case CheckBetween:
                            processCheckBetweenConstraint(constraint, tableName);
                            break;
                        case CheckNull:
                            processCheckNullConstraint(constraint, tableName);
                            break;
                        case Unique:
                            processUniqueConstraint(constraint, tableName);
                            break;
                        default:
                            throw new StatementResolverException(String.format("%s: Expected a valid constraint type.", this.getClass().getSimpleName()));
                    }
                }
            }
        }
    }

    /**
     * This method is processing the primary key constraint and prepares the
     * statements for the sub tables.
     *
     * @param constraint
     * @param tableName
     * @param columnsByName
     * @throws StatementResolverException
     */
    private void processPrimaryKeyConstraint(Constraint constraint, String tableName,
            HashMap<String, Column> columnsByName) throws StatementResolverException {
        if (constraint == null) {
            throw new StatementResolverException(String.format("%s: No constraint was passed for primary key processing.", this.getClass().getSimpleName()));
        } else if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else {
            String primaryKeyConstraintName = constraint.getName();
            String primaryKeyAttributeName = constraint.getAttributeName();
            int primaryKeyConstraintDB = constraint.getDb();
            if (isVerticallyPartitioned(tableName)) {
                if (dbRelatedColumns == null || dbRelatedColumns.isEmpty()) {
                    throw new StatementResolverException(String.format("%s: Vertical partitioning does not exist for this fed-table.", this.getClass().getSimpleName()));
                } else {
                    String constraintStatement;
                    constraintStatement = String.format("CONSTRAINT %s PRIMARY KEY (%s)",
                            primaryKeyConstraintName, primaryKeyAttributeName);
                    Column primaryKeyColumn = columnsByName.get(primaryKeyAttributeName);
                    String primaryKeyType = primaryKeyColumn.getType();
                    String primaryKeyAttributeStatement;
                    int primaryKeyColumnDb = primaryKeyColumn.getDatabase();
                    primaryKeyAttributeStatement = String.format("%s %s", primaryKeyAttributeName, primaryKeyType);
                    if (verticalPartitions == 2) {
                        if (pkAttrAdded == false) {
                            switch (primaryKeyColumnDb) {
                                case 1:
                                    executableStatementsListDB2.add(0, primaryKeyAttributeStatement);
                                    pkAttrAdded = true;
                                    break;
                                case 2:
                                    executableStatementsListDB1.add(0, primaryKeyAttributeStatement);
                                    pkAttrAdded = true;
                                    break;
                            }
                        }
                        if (pkCanAdded == false) {
                            addStatementsToTwoLists(constraintStatement, 1, 2);
                            pkCanAdded = true;
                        }
                    }
                    if (verticalPartitions == 3) {
                        if (pkAttrAdded == false) {
                            switch (primaryKeyColumnDb) {
                                case 1:
                                    executableStatementsListDB2.add(0, primaryKeyAttributeStatement);
                                    executableStatementsListDB3.add(0, primaryKeyAttributeStatement);
                                    pkAttrAdded = true;
                                    break;
                                case 2:
                                    executableStatementsListDB1.add(0, primaryKeyAttributeStatement);
                                    executableStatementsListDB3.add(0, primaryKeyAttributeStatement);
                                    pkAttrAdded = true;
                                    break;
                                case 3:
                                    executableStatementsListDB1.add(0, primaryKeyAttributeStatement);
                                    executableStatementsListDB2.add(0, primaryKeyAttributeStatement);
                                    pkAttrAdded = true;
                                    break;
                            }
                        }
                        if (pkCanAdded == false) {
                            addStatementsToAllLists(constraintStatement);
                            pkCanAdded = true;
                        }
                    }
                }
            } else if (isHorizontallyPartitioned(tableName)) {
                String constraintStatement;
                constraintStatement = String.format("CONSTRAINT %s PRIMARY KEY (%s)",
                        primaryKeyConstraintName, primaryKeyAttributeName);
                switch (primaryKeyConstraintDB) {
                    case 1:
                        executableStatementsListDB1.add(constraintStatement);
                        break;
                    case 2:
                        executableStatementsListDB2.add(constraintStatement);
                        break;
                    case 3:
                        executableStatementsListDB3.add(constraintStatement);
                        break;
                    case 0:
                        addStatementsToAllLists(constraintStatement);
                        break;
                }
            } else {
                String constraintStatement;
                constraintStatement = String.format("CONSTRAINT %s PRIMARY KEY (%s)",
                        primaryKeyConstraintName, primaryKeyAttributeName);
                executableStatementsListDB1.add(constraintStatement);
            }
        }
    }

    /**
     * This method is processing the foreign key constraints for the attributes.
     *
     * @param constraint
     * @param tableName
     * @throws StatementResolverException
     */
    private void processForeignKeyConstraint(Constraint constraint, String tableName) throws StatementResolverException {
        if (constraint == null) {
            throw new StatementResolverException(String.format("%s: No constraint was passed for foreign key processing.", this.getClass().getSimpleName()));
        } else if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else {
            String fKeyAttribConstraintName = constraint.getName();
            String fKeyAttribName = constraint.getAttributeName();
            String fKeyAttribRefTable = constraint.getTableReference();
            String fKeyAttribRefAttrib = constraint.getTableReferenceAttribute();
            String constraintStatement;
            constraintStatement = String.format("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
                    fKeyAttribConstraintName, fKeyAttribName, fKeyAttribRefTable, fKeyAttribRefAttrib);

            if (isVerticallyPartitioned(tableName)) {
                if (isVerticallyPartitioned(fKeyAttribRefTable)) {
                    int fkeyAttribDb = metadataManager.getColumn(tableName, fKeyAttribName).getDatabase();
                    int refTableCount = checkDatabaseCountforRefTable(fKeyAttribRefTable);
                    if (refTableCount == 2 && fkeyAttribDb == 3) {
                    } else {
                        switch (fkeyAttribDb) {
                            case 1:
                                executableStatementsListDB1.add(constraintStatement);
                                break;
                            case 2:
                                executableStatementsListDB2.add(constraintStatement);
                                break;
                            case 3:
                                executableStatementsListDB3.add(constraintStatement);
                                break;
                        }
                    }
                } else if (isHorizontallyPartitioned(fKeyAttribRefTable)) {
                } else {
                    int fkeyAttribDb = metadataManager.getColumn(tableName, fKeyAttribName).getDatabase();
                    if (fkeyAttribDb == 1) {
                        executableStatementsListDB1.add(constraintStatement);
                    }
                }
            } else if (isHorizontallyPartitioned(tableName)) {
                if (isVerticallyPartitioned(fKeyAttribRefTable)) {
                    int refTableCount = checkDatabaseCountforRefTable(fKeyAttribRefTable);
                    if (horizontalPartitions <= refTableCount) {
                        if (horizontalPartitions == 2) {
                            switch (constraint.getDb()) {
                                case 0:
                                    addStatementsToTwoLists(constraintStatement, 1, 2);
                                    break;
                                case 1:
                                    executableStatementsListDB1.add(constraintStatement);
                                    break;
                                case 2:
                                    executableStatementsListDB2.add(constraintStatement);
                                    break;
                            }
                        }
                        if (horizontalPartitions == 3) {
                            switch (constraint.getDb()) {
                                case 0:
                                    addStatementsToAllLists(constraintStatement);
                                    break;
                                case 1:
                                    executableStatementsListDB1.add(constraintStatement);
                                    break;
                                case 2:
                                    executableStatementsListDB2.add(constraintStatement);
                                    break;
                                case 3:
                                    executableStatementsListDB3.add(constraintStatement);
                                    break;
                            }
                        }
                    } else {
                        switch (constraint.getDb()) {
                            case 1:
                                executableStatementsListDB1.add(constraintStatement);
                                break;
                            case 2:
                                executableStatementsListDB2.add(constraintStatement);
                                break;
                            case 3:
                                executableStatementsListDB3.add(constraintStatement);
                                break;
                            case 0:
                                addStatementsToTwoLists(constraintStatement, 1, 2);
                        }
                    }
                } else if (isHorizontallyPartitioned(fKeyAttribRefTable)) {
                } else {
                    if (!isVerticallyPartitioned(fKeyAttribRefTable) && !isHorizontallyPartitioned(fKeyAttribRefTable)) {
                        executableStatementsListDB1.add(constraintStatement);
                    } else {
                        if (isVerticallyPartitioned(fKeyAttribRefTable)) {
                            executableStatementsListDB1.add(constraintStatement);
                        }
                    }
                }
            }
        }
    }

    /**
     * This method is processing the unique constraints for the attributes.
     *
     * @param constraint
     * @param tableName
     * @throws StatementResolverException
     */
    private void processUniqueConstraint(Constraint constraint, String tableName) throws StatementResolverException {
        if (constraint == null) {
            throw new StatementResolverException(String.format("%s: No constraint passed to check for not null.", this.getClass().getSimpleName()));
        } else if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else {
            String uniqueAttributeName = constraint.getAttributeName();
            String uniqueConstraintName = constraint.getName();
            String constraintStatement;
            constraintStatement = String.format("CONSTRAINT %s UNIQUE (%s)",
                    uniqueConstraintName, uniqueAttributeName);
            if (isVerticallyPartitioned(tableName)) {
                switch (constraint.getDb()) {
                    case 1:
                        executableStatementsListDB1.add(constraintStatement);
                        break;
                    case 2:
                        executableStatementsListDB2.add(constraintStatement);
                        break;
                    case 3:
                        executableStatementsListDB3.add(constraintStatement);
                        break;
                    case 0:
                        if (verticalPartitions == 2) {
                            addStatementsToTwoLists(constraintStatement, 1, 2);
                        } else {
                            addStatementsToAllLists(constraintStatement);
                        }
                        break;
                }
            } else if (isHorizontallyPartitioned(tableName)) {
                switch (constraint.getDb()) {
                    case 1:
                        executableStatementsListDB1.add(constraintStatement);
                        break;
                    case 2:
                        executableStatementsListDB2.add(constraintStatement);
                        break;
                    case 3:
                        executableStatementsListDB3.add(constraintStatement);
                        break;
                    case 0:
                        if (horizontalPartitions == 2) {
                            addStatementsToTwoLists(constraintStatement, 1, 2);
                        } else {
                            addStatementsToAllLists(constraintStatement);
                        }
                        break;
                }
            } else {
                executableStatementsListDB1.add(constraintStatement);
            }
        }
    }

    /**
     * This method is processing the check comparison constraints for the
     * attributes.
     *
     * @param constraint
     * @param tableName
     * @throws StatementResolverException
     */
    private void processCheckComparisonConstraint(Constraint constraint, String tableName) throws StatementResolverException {
        if (constraint == null) {
            throw new StatementResolverException(String.format("%s: No constraint was passed for comparison check processing.", this.getClass().getSimpleName()));
        } else if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else {
            String comparisonConstraintName = constraint.getName();
            BinaryConstraint binaryConstraint = (BinaryConstraint) constraint;
            BinaryType binaryConstraintType = binaryConstraint.getBinaryType();
            String constraintLeftColumnName = binaryConstraint.getLeftColumnName();
            String constraintRightLiteralName = binaryConstraint.getRightLiteralValue();
            String constraintRightLiteralType = binaryConstraint.getRightLiteralType();
            String constraintOperator = binaryConstraint.getBinaryOperator();
            int constraintDb = constraint.getDb();
            int comparisonAttrDb = metadataManager.getColumn(tableName, constraintLeftColumnName).getDatabase();
            String constraintStatement;
            if (constraintRightLiteralType.contains("VARCHAR")) {
                if (binaryConstraintType == BinaryType.attribute_attribute) {
                    constraintStatement = String.format("CONSTRAINT %s CHECK (%s %s %s)", comparisonConstraintName,
                            constraintLeftColumnName, constraintOperator, constraintRightLiteralName);
                } else {
                    constraintStatement = String.format("CONSTRAINT %s CHECK (%s %s '%s')", comparisonConstraintName,
                            constraintLeftColumnName, constraintOperator, constraintRightLiteralName);
                }
            } else {
                constraintStatement = String.format("CONSTRAINT %s CHECK (%s %s %s)", comparisonConstraintName,
                        constraintLeftColumnName, constraintOperator, constraintRightLiteralName);
            }
            if (binaryConstraint.getBinaryType() == BinaryType.attribute_attribute) {
                int rightComparisonAttrDb = metadataManager.getColumn(tableName, constraintRightLiteralName).getDatabase();
                if (isVerticallyPartitioned(tableName)) {
                    if (comparisonAttrDb == rightComparisonAttrDb) {
                        switch (comparisonAttrDb) {
                            case 1:
                                executableStatementsListDB1.add(constraintStatement);
                                break;
                            case 2:
                                executableStatementsListDB2.add(constraintStatement);
                                break;
                            case 3:
                                executableStatementsListDB3.add(constraintStatement);
                                break;
                            default:
                                throw new StatementResolverException(String.format("%s: The referenced database for this constraint does not exist.", this.getClass().getSimpleName()));
                        }
                    }
                } else if (isHorizontallyPartitioned(tableName)) {

                    switch (constraintDb) {
                        case 1:
                            executableStatementsListDB1.add(constraintStatement);
                            break;
                        case 2:
                            executableStatementsListDB2.add(constraintStatement);
                            break;
                        case 3:
                            executableStatementsListDB3.add(constraintStatement);
                            break;
                        default:
                            throw new StatementResolverException(String.format("%s: The referenced database for this constraint does not exist.", this.getClass().getSimpleName()));
                    }
                } else {
                    executableStatementsListDB1.add(constraintStatement);
                }
            } else {
                if (isVerticallyPartitioned(tableName)) {
                    switch (comparisonAttrDb) {
                        case 1:
                            executableStatementsListDB1.add(constraintStatement);
                            break;
                        case 2:
                            executableStatementsListDB2.add(constraintStatement);
                            break;
                        case 3:
                            executableStatementsListDB3.add(constraintStatement);
                            break;
                        default:
                            throw new StatementResolverException(String.format("%s: The referenced database for this constraint does not exist.", this.getClass().getSimpleName()));
                    }
                } else if (isHorizontallyPartitioned(tableName)) {
                    switch (constraintDb) {
                        case 1:
                            executableStatementsListDB1.add(constraintStatement);
                            break;
                        case 2:
                            executableStatementsListDB2.add(constraintStatement);
                            break;
                        case 3:
                            executableStatementsListDB3.add(constraintStatement);
                            break;
                        default:
                            throw new StatementResolverException(String.format("%s: The referenced database for this constraint does not exist.", this.getClass().getSimpleName()));
                    }
                } else {
                    executableStatementsListDB1.add(constraintStatement);
                }
            }
        }
    }

    /**
     * This method is processing the check between constraint for the
     * attributes.
     *
     * @param constraint
     * @param tableName
     * @throws StatementResolverException
     */
    private void processCheckBetweenConstraint(Constraint constraint, String tableName) throws StatementResolverException {
        if (constraint == null) {
            throw new StatementResolverException(String.format("%s: No constraint was passed for between check processing.", this.getClass().getSimpleName()));
        } else if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else {
            String betwConstraintName = constraint.getName();
            String betwConstraintAttr = constraint.getAttributeName();
            BetweenConstraint betwConstraint = (BetweenConstraint) constraint;
            String minValue = betwConstraint.getMinValue();
            String maxValue = betwConstraint.getMaxValue();
            int constraintDb = constraint.getDb();
            String constraintStatement;
            constraintStatement = String.format("CONSTRAINT %s CHECK (%s BETWEEN %s AND %s)",
                    betwConstraintName, betwConstraintAttr, minValue, maxValue);
            if (isVerticallyPartitioned(tableName) || isHorizontallyPartitioned(tableName)) {
                switch (constraintDb) {
                    case 1:
                        executableStatementsListDB1.add(constraintStatement);
                        break;
                    case 2:
                        executableStatementsListDB2.add(constraintStatement);
                        break;
                    case 3:
                        executableStatementsListDB3.add(constraintStatement);
                        break;
                    default:
                        throw new StatementResolverException(String.format("%s: The referenced database for this constraint does not exist.", this.getClass().getSimpleName()));
                }
            } else {
                executableStatementsListDB1.add(constraintStatement);
            }
        }
    }

    /**
     * This method is processing the check not null constraints for the
     * attributes.
     *
     * @param constraint
     * @param tableName
     * @throws StatementResolverException
     */
    private void processCheckNullConstraint(Constraint constraint, String tableName) throws StatementResolverException {
        if (constraint == null) {
            throw new StatementResolverException(String.format("%s: No constraint was passed for null check processing.", this.getClass().getSimpleName()));
        } else if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else {
            String notNullConstraintName = constraint.getName();
            String notNullConstraintAttr = constraint.getAttributeName();
            String constraintStatement;
            int constraintDb = constraint.getDb();
            constraintStatement = String.format("CONSTRAINT %s CHECK (%s IS NOT NULL)",
                    notNullConstraintName, notNullConstraintAttr);
            if (isVerticallyPartitioned(tableName) || isHorizontallyPartitioned(tableName)) {
                switch (constraintDb) {
                    case 1:
                        executableStatementsListDB1.add(constraintStatement);
                        break;
                    case 2:
                        executableStatementsListDB2.add(constraintStatement);
                        break;
                    case 3:
                        executableStatementsListDB3.add(constraintStatement);
                        break;
                    default:
                        throw new StatementResolverException(String.format("%s: The referenced database for this constraint does not exist.", this.getClass().getSimpleName()));
                }
            } else {
                executableStatementsListDB1.add(constraintStatement);
            }
        }
    }

    /**
     * Check if the fed-table is vertically partitioned.
     *
     * @param tableName
     * @return
     * @throws StatementResolverException
     */
    private boolean isVerticallyPartitioned(String tableName) throws StatementResolverException {
        if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else {
            return metadataManager.getPartitionType(tableName) == PartitionType.Vertical;
        }
    }

    /**
     * Check if the fed-table is horizontally partitioned
     *
     * @param tableName
     * @return
     * @throws StatementResolverException
     */
    private boolean isHorizontallyPartitioned(String tableName) throws StatementResolverException {
        if (tableName == null || tableName.isEmpty()) {
            throw new StatementResolverException(String.format("%s: No table name was provided for this fed-table.", this.getClass().getSimpleName()));
        } else {
            return metadataManager.getPartitionType(tableName) == PartitionType.Horizontal;
        }
    }

    /**
     * implementation of a containsIgnoreCase method
     *
     * @param str, String
     * @param searchStr, String
     * @return boolean
     */
    public static boolean containsIgnoreCase(String str, String searchStr) {
        if (str == null || searchStr == null) {
            return false;
        }

        final int length = searchStr.length();
        if (length == 0) {
            return true;
        }

        for (int i = str.length() - length; i >= 0; i--) {
            if (str.regionMatches(true, i, searchStr, 0, length)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks the count of the partitions and returns an integer value.
     *
     * @param tableName
     * @return int
     */
    private int checkDatabaseCountforRefTable(String tableName) {
        HashMap<Integer, LinkedHashMap<String, Column>> refTableDbRelatedColumns;
        refTableDbRelatedColumns = metadataManager.getTable(tableName).getDatabaseRelatedColumns();
        int referencedTablePartitions = 0;
        Iterator<Map.Entry<Integer, LinkedHashMap<String, Column>>> parent = refTableDbRelatedColumns.entrySet().iterator();
        while (parent.hasNext()) {
            Map.Entry<Integer, LinkedHashMap<String, Column>> parentPair = parent.next();
            referencedTablePartitions = 2;
            if (parentPair.getKey() == 3 && !parentPair.getValue().isEmpty()) {
                if (referencedTablePartitions == 2) {
                    referencedTablePartitions = 3;
                }
            }
        }
        return referencedTablePartitions;
    }

    /**
     * Helper method to add a string statement to two specific executable
     * statements lists for the sub tasks.
     *
     * @param statement
     */
    private void addStatementsToTwoLists(String statement, int first, int second) {
        if (first == 1 && second == 2) {
            executableStatementsListDB1.add(statement);
            executableStatementsListDB2.add(statement);
        } else if (first == 1 && second == 3) {
            executableStatementsListDB1.add(statement);
            executableStatementsListDB3.add(statement);
        } else {
            executableStatementsListDB2.add(statement);
            executableStatementsListDB3.add(statement);
        }
    }

    /**
     * Helper method to add a string statement to all executable statements
     * lists for the sub tasks.
     *
     * @param statement
     */
    private void addStatementsToAllLists(String statement) {
        executableStatementsListDB1.add(statement);
        executableStatementsListDB2.add(statement);
        executableStatementsListDB3.add(statement);
    }
}
