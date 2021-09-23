package fdbs.sql.semantic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.*;
import fdbs.sql.parser.ast.*;
import fdbs.sql.parser.ast.clause.WhereClause;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.identifier.AttributeIdentifier;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.literal.Literal;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.literal.StringLiteral;
import fdbs.sql.parser.ast.visitor.*;
import fdbs.sql.parser.ast.statement.*;
import fdbs.sql.parser.ast.statement.select.*;
import fdbs.sql.parser.ast.type.PrimitiveType;
import fdbs.util.logger.Logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

/**
 * A semantic validator which makes inter alia type checks and integrity checks.
 */
public class SemanticValidator extends ASTVisitor {

    protected MetadataManager metadataManager;
    protected FedConnection fedConnection;

    public SemanticValidator(FedConnection fedConnection) throws FedException {
        this.metadataManager = MetadataManager.getInstance(fedConnection);
        this.fedConnection = fedConnection;
    }

    @Override
    public void willVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Start statement semantic validation.", this.getClass().getSimpleName()));
    }

    @Override
    public boolean visit(ASTNode node) throws SemanticValidationException, Exception {
        boolean visitChilds = true;
        SemanticValidator stmtValidator = null;

        if (node instanceof CreateTableStatement) {
            stmtValidator = new CreateTableStatementSemanticValidator(fedConnection);
        } else if (node instanceof DropTableStatement) {
            stmtValidator = new DropTableStatementSemanticValidator(fedConnection);
        } else if (node instanceof InsertStatement) {
            stmtValidator = new InsertStatementSemanticValidator(fedConnection);
        } else if (node instanceof UpdateStatement) {
            stmtValidator = new UpdateStatementSemanticValidator(fedConnection);
        } else if (node instanceof DeleteStatement) {
            stmtValidator = new DeleteStatementSemanticValidator(fedConnection);
        } else if (node instanceof SelectStatement) {
            stmtValidator = new SelectStatementSemanticValidator(fedConnection);
        } else if (node instanceof Statement) {
            throw new SemanticValidationException(String.format("%s: Statement \"%s\" has a unknown statement type.", this.getClass().getSimpleName(), node.getClass().getName()));
        }

        if (stmtValidator != null) {
            visitChilds = false;
            node.accept(stmtValidator);
        }

        return visitChilds;
    }

    @Override
    public void didVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Finished statement semantic validation.", this.getClass().getSimpleName()));
    }

    // <editor-fold defaultstate="collapsed" desc="Private Helper Functions" >
    protected Table processTableName(String tableName, boolean shouldExists) throws SemanticValidationException {
        Table table = metadataManager.getTable(tableName);

        if (!shouldExists && table != null) {
            throw new SemanticValidationException(String.format("%s: A table with the name \"%s\" does already exists.", this.getClass().getSimpleName(), tableName));
        } else if (shouldExists && table == null) {
            throw new SemanticValidationException(String.format("%s: A table with the name \"%s\" does not exists.", this.getClass().getSimpleName(), tableName));
        }

        return table;
    }

    // <editor-fold defaultstate="collapsed" desc="Check Where Clause Helper Functions">
    protected void checkWhereClause(HashMap<String, Table> tableInfos, WhereClause whereClause) throws SemanticValidationException {
        BinaryExpression binaryExpression = whereClause.getBinaryExpression();
        checkWhereBinaryExpression(tableInfos, binaryExpression);
    }

    private void checkWhereBinaryExpression(HashMap<String, Table> tableInfos, BinaryExpression binaryExpression) throws SemanticValidationException {
        if (binaryExpression.isJoinCondition()) {
            checkJoinCondition(tableInfos, binaryExpression);
        } else if (binaryExpression.isNoNJoinCondition()) {
            checkAttributeConstantBinaryExpression(tableInfos, binaryExpression);
        } else if (binaryExpression.isANestedBinaryExpression()) {
            checkWhereBinaryExpression(tableInfos, (BinaryExpression) binaryExpression.getLeftOperand());
            checkWhereBinaryExpression(tableInfos, (BinaryExpression) binaryExpression.getRightOperand());
        } else {
            throw new SemanticValidationException(String.format("%s: Where clause containts a not supported binary expression: %s %s %s.", this.getClass().getSimpleName(),
                    binaryExpression.getLeftOperand().getClass().getName(), binaryExpression.getOperatorName(), binaryExpression.getRightOperand().getClass().getName()));
        }
    }

    private void checkJoinCondition(HashMap<String, Table> tableInfos, BinaryExpression binaryExpression) throws SemanticValidationException {
        ASTNode leftOperand = binaryExpression.getLeftOperand();
        ASTNode rightOperand = binaryExpression.getRightOperand();

        if (leftOperand instanceof FullyQualifiedAttributeIdentifier && rightOperand instanceof FullyQualifiedAttributeIdentifier) {
            checkSingleFullQualifiedAttribute(tableInfos, (FullyQualifiedAttributeIdentifier) leftOperand, "Where clause - ");
            checkSingleFullQualifiedAttribute(tableInfos, (FullyQualifiedAttributeIdentifier) rightOperand, "Where clause - ");
            // no type check required because values from tables can implicit converted while a join and we have just the two types INTEGER AND VARCHAR
        } else {
            throw new SemanticValidationException(String.format("%s: Only a binary expression with a fully qualified attribute - fully qualified attribute comparison is allowed in a select join condition.", this.getClass().getSimpleName()));
        }
    }

    private void checkAttributeConstantBinaryExpression(HashMap<String, Table> tableInfos, BinaryExpression binaryExpression) throws SemanticValidationException {
        ASTNode leftOperand = binaryExpression.getLeftOperand();
        ASTNode rightOperand = binaryExpression.getRightOperand();

        if ((leftOperand instanceof AttributeIdentifier || leftOperand instanceof FullyQualifiedAttributeIdentifier) && rightOperand instanceof Literal) {
            String attributeName = "";
            Table table = null;
            String tableName = "";
            Column column = null;

            if (leftOperand instanceof AttributeIdentifier) {
                // Insert WHERE
                AttributeIdentifier lefAttribute = (AttributeIdentifier) leftOperand;
                attributeName = lefAttribute.getIdentifier();
                for (String key : tableInfos.keySet()) {
                    // Insert Statement have just one table
                    table = tableInfos.get(key);
                    tableName = key;
                    column = table.getColumn(attributeName);
                    break;
                }
            } else if (leftOperand instanceof FullyQualifiedAttributeIdentifier) {
                // SELECT WHERE
                FullyQualifiedAttributeIdentifier fQAttribute = (FullyQualifiedAttributeIdentifier) leftOperand;
                tableName = fQAttribute.getIdentifier();
                attributeName = fQAttribute.getAttributeIdentifier().getIdentifier();

                table = tableInfos.get(tableName);

                if (table == null) {
                    throw new SemanticValidationException(String.format("%s: Where clause - The table name \"%s\" from the full qualified attribute \"%s.%s\" is unknown.", this.getClass().getSimpleName(), tableName, tableName, attributeName));
                }

                column = table.getColumn(attributeName);
            }

            if (table == null) {
                throw new SemanticValidationException(String.format("%s: Where clause fatal error - The table for the attribute \"%s\" does not exists.", this.getClass().getSimpleName(), attributeName, tableName));
            }

            if (column == null) {
                throw new SemanticValidationException(String.format("%s: Where clause - The attribute \"%s\" does not exists in the table \"%s\".", this.getClass().getSimpleName(), attributeName, tableName));
            }

//            if (rightOperand instanceof NullLiteral && !table.isColumnNullAble(attributeName)){
//                throw new SemanticValidationException(String.format("%s: Where clause - Null check constraint violation - The value from the attribute \"%s.%s\" is null.", this.getClass().getSimpleName(), tableName, attributeName));
//            }
            if (!isValueStrictTypeOfColumn(column.getType(), (Literal) rightOperand)) {
                throw new SemanticValidationException(String.format("%s: Where clause - The type from the attribute \"%s.%s\" is not equals the type of the value.", this.getClass().getSimpleName(), tableName, attributeName));
            }
        } else {
            throw new SemanticValidationException(String.format("%s: Only a binary expression with a (fully qualified) attribute and constant comparison is allowed in a data manipulation where clause or in a select non join condition.", this.getClass().getSimpleName()));
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Check Types Helper Functions">
    protected boolean isValueStrictTypeOfColumn(String columnType, Literal value) {
        boolean isTypeOf = false;

        if (value instanceof StringLiteral && columnType.startsWith("VARCHAR")) {
            isTypeOf = true;
        } else if (value instanceof IntegerLiteral && columnType.equals(PrimitiveType.Primitive.INTEGER.name())) {
            isTypeOf = true;
        } else if (value instanceof NullLiteral) {
            isTypeOf = true;
        }

        return isTypeOf;
    }

    protected boolean isValueTypeOfColumnOrCanBeConverted(String columnType, Literal value) {
        boolean isTypeOfOrCanBeConverted = false;

        if (columnType.equals(PrimitiveType.Primitive.INTEGER.name()) && value instanceof IntegerLiteral) {
            isTypeOfOrCanBeConverted = true;
        } else if (columnType.equals(PrimitiveType.Primitive.INTEGER.name()) && value instanceof StringLiteral) {
            try {
                Integer.parseInt(value.getIdentifier());
                isTypeOfOrCanBeConverted = true;
            } catch (NumberFormatException ex) {
            }
        } else if (columnType.startsWith("VARCHAR") && (value instanceof StringLiteral || value instanceof IntegerLiteral)) {
            isTypeOfOrCanBeConverted = true;
        } else if (value instanceof NullLiteral) {
            isTypeOfOrCanBeConverted = true;
        }

        return isTypeOfOrCanBeConverted;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Check Integrity Conditions Helper Functions">
    protected void checkIntegrityConditions(List<Constraint> constraints, Table table, HashMap<String, String> attributeNameValueMapping, boolean isUpdate, WhereClause updateWhere) throws SemanticValidationException, FedException {
        for (Constraint constraint : constraints) {
            String constraintName = constraint.getName();
            String constraintClassName = constraint.getClass().getName();

            if (constraint.getDb() == 0) {
                if (null == constraint.getType()) {
                    throw new SemanticValidationException(String.format("%s: The constraint type \"%s\" from the constraint \"%s\" is unknown.", this.getClass().getSimpleName(), constraintName, constraintClassName));
                } else {
                    switch (constraint.getType()) {
                        case PrimaryKey:
                        case Unique:
                            if (isUpdate) {
                                checkUniqueOrPrimaryKeyIntegrityConditionForUpdate(constraint, table, attributeNameValueMapping, updateWhere);
                            } else {
                                checkUniqueOrPrimaryKeyIntegrityConditionForInsert(constraint, table, attributeNameValueMapping);
                            }
                            break;
                        case ForeignKey:
                            // Process the where claue in a update is not required because foreign key value can be set mulitple times
                            checkForeignKeyIntegrityCondition(constraint, attributeNameValueMapping);
                            break;
                        //Database check all not-null and between check constraints 
                        case CheckComparison:
                            if (isUpdate && updateWhere == null) {
                                checkUpdateWithoutWehreAttributeAttributeComparisonIntegrityCheck(constraint, table, attributeNameValueMapping);
                            } else {
                                checkInsertAttributeAttributeComparisonIntegrityCheck(constraint, table, attributeNameValueMapping);
                            }
                            break;
                        default:
                            throw new SemanticValidationException(String.format("%s: The constraint type \"%s\" from the constraint \"%s\" is unknown.", this.getClass().getSimpleName(), constraintName, constraintClassName));
                    }
                }
            }
        }
    }

    private void checkForeignKeyIntegrityCondition(Constraint constraint, HashMap<String, String> attributeNameValueMapping) throws SemanticValidationException, FedException {
        String constraintName = constraint.getName();
        String constraintClassName = constraint.getClass().getName();

        if (constraint.getType() != ConstraintType.ForeignKey) {
            throw new SemanticValidationException(String.format("%s: Foreign Key constraint \"%s\" validation - The foreign key constraint type \"%s\" is unknown.", this.getClass().getSimpleName(), constraintName, constraintClassName));
        }

        String attributeName = constraint.getAttributeName();

        // check is here for update statements which maybe does not update a foreign key
        if (attributeNameValueMapping.containsKey(attributeName)) {
            String attributeValue = attributeNameValueMapping.get(attributeName);
            if (attributeValue != null) {
                Table refTable = metadataManager.getTable(constraint.getTableReference());
                String refTableName = refTable.getAttributeName();
                Column refColumn = refTable.getColumn(constraint.getTableReferenceAttribute());
                String refAttributeName = refColumn.getAttributeName();
                String refAttributeType = refColumn.getType();

                String query = "";
                if (refAttributeType.startsWith("VARCHAR")) {
                    query = String.format("SELECT Count(*) FROM %s WHERE %s = '%s'", refTableName, refAttributeName, attributeValue);
                } else if (refAttributeType.equals(PrimitiveType.Primitive.INTEGER.name())) {
                    query = String.format("SELECT Count(*) FROM %s WHERE %s = %s", refTableName, refAttributeName, attributeValue);
                } else {
                    throw new SemanticValidationException(String.format("%s: Foreign Key constraint \"%s\" validation - The foreign key attribute \"%s\" has a unknown type.", this.getClass().getSimpleName(), constraintName, refAttributeName));
                }

                boolean isForeignKeyAvailable = false;

                try {
                    HashMap<Integer, Connection> connections = fedConnection.getConnections();
                    List<Integer> databasesForColumn = refTable.getDatabasesForColumn(refAttributeName);

                    if (databasesForColumn.isEmpty()) {
                        throw new SemanticValidationException(String.format("%s: Foreign Key constraint \"%s\" validation - The foreign key attribute \"%s\" could not be assigned to any database.", this.getClass().getSimpleName(), constraintName, refAttributeName));
                    }

                    for (int dbID : databasesForColumn) {
                        Connection connection = connections.get(dbID);
                        Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), dbID, query));
                        try (java.sql.Statement statement = connection.createStatement();
                                ResultSet result = statement.executeQuery(query)) {
                            if (result.next()) {
                                int count = result.getInt(1);
                                if (count > 0) {
                                    isForeignKeyAvailable |= true;
                                }
                                if (isForeignKeyAvailable) {
                                    break;
                                }
                            }
                        }
                    }
                } catch (SQLException ex) {
                    throw new SemanticValidationException(String.format("%s: Foreign Key constraint \"%s\" validation - Querying underlying databases failed.%n%s", this.getClass().getSimpleName(), constraintName, ex.getMessage()), ex);
                }

                if (!isForeignKeyAvailable) {
                    throw new SemanticValidationException(String.format("%s: Foreign Key constraint \"%s\" validation - The foreign key constraint value \"%s\" is unknown.", this.getClass().getSimpleName(), constraintName, attributeValue));
                }
            }
        }
    }

    // <editor-fold defaultstate="collapsed" desc="Check Primary Key and Unique Integrity Conditions Functions">
    private void checkUniqueOrPrimaryKeyIntegrityConditionForInsert(Constraint constraint, Table table, HashMap<String, String> attributeNameValueMapping) throws SemanticValidationException, FedException {
        ConstraintType constraintType = constraint.getType();
        String constraintTypeName = constraintType.name();
        String constraintName = constraint.getName();
        String constraintClassName = constraint.getClass().getName();

        if (!(constraintType == ConstraintType.Unique || constraintType == ConstraintType.PrimaryKey)) {
            throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an insert - The %s constraint type \"%s\" is unknown.", this.getClass().getSimpleName(), constraintTypeName, constraintTypeName.toLowerCase(), constraintName, constraintClassName));
        }

        String uniqueAttributeName = constraint.getAttributeName();

        if (attributeNameValueMapping.containsKey(uniqueAttributeName)) {
            HashMap<Integer, Connection> connections = fedConnection.getConnections();
            List<Integer> databasesForColumn = table.getDatabasesForColumn(uniqueAttributeName);

            String tableName = table.getAttributeName();
            Column uniqueColumn = table.getColumn(uniqueAttributeName);
            String uniqueAttributeType = uniqueColumn.getType();
            String settingValue = attributeNameValueMapping.get(uniqueAttributeName);

            if (databasesForColumn.isEmpty()) {
                throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an insert - The %s attribute \"%s\" could not be assigned to any database.", this.getClass().getSimpleName(), constraintTypeName, constraintName, constraintTypeName.toLowerCase(), uniqueAttributeName));
            }

            String query = "";
            if (uniqueAttributeType.startsWith("VARCHAR")) {
                query = String.format("SELECT Count(*) FROM %s WHERE %s = '%s'", tableName, uniqueAttributeName, settingValue);
            } else if (uniqueAttributeType.equals(PrimitiveType.Primitive.INTEGER.name())) {
                query = String.format("SELECT Count(*) FROM %s WHERE %s = %s", tableName, uniqueAttributeName, settingValue);
            } else {
                throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an insert - The \"%s\" attribute \"%s\" has a unknown type.", this.getClass().getSimpleName(), constraintTypeName, constraintName, constraintTypeName.toLowerCase(), uniqueAttributeName));
            }

            boolean insertOrUpdateIsPossible = true;

            try {
                for (int dbID : databasesForColumn) {
                    Connection connection = connections.get(dbID);
                    Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), dbID, query));
                    try (java.sql.Statement statement = connection.createStatement();
                            ResultSet result = statement.executeQuery(query);) {
                        if (result.next()) {
                            int count = result.getInt(1);
                            if (count > 0) {
                                insertOrUpdateIsPossible &= false;
                            }
                        }
                    }
                }
            } catch (SQLException ex) {
                throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an insert - Querying underlying databases failed.%n%s", this.getClass().getSimpleName(), constraintTypeName, constraintName, ex.getMessage()), ex);
            }

            if (!insertOrUpdateIsPossible) {
                throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an insert - The value \"%s\" is not unique to the %s constraint \"%s\".", this.getClass().getSimpleName(), constraintTypeName, constraintName, settingValue, constraintTypeName.toLowerCase(), constraintName));
            }
        }
    }

    private void checkUniqueOrPrimaryKeyIntegrityConditionForUpdate(Constraint constraint, Table table, HashMap<String, String> attributeNameValueMapping, WhereClause whereClause) throws SemanticValidationException, FedException {
        ConstraintType constraintType = constraint.getType();
        String constraintTypeName = constraintType.name();
        String constraintName = constraint.getName();
        String constraintClassName = constraint.getClass().getName();

        if (!(constraintType == ConstraintType.Unique || constraintType == ConstraintType.PrimaryKey)) {
            throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an update - The %s constraint type \"%s\" is unknown.", this.getClass().getSimpleName(), constraintTypeName, constraintTypeName.toLowerCase(), constraintName, constraintClassName));
        }

        String uniqueAttributeName = constraint.getAttributeName();

        if (attributeNameValueMapping.containsKey(uniqueAttributeName)) {
            List<Integer> databasesForColumn = table.getDatabasesForColumn(uniqueAttributeName);
            if (databasesForColumn.isEmpty()) {
                throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an update - The %s attribute \"%s\" could not be assigned to any database.", this.getClass().getSimpleName(), constraintTypeName, constraintName, constraintTypeName.toLowerCase(), uniqueAttributeName));
            }

            String tableName = table.getAttributeName();
            String settingValue = attributeNameValueMapping.get(uniqueAttributeName);

            if (whereClause == null) {
                checkUniqueOrPrimaryKeyIntegrityConditionForUpdateWithOutWhere(tableName, constraintTypeName, constraintName, databasesForColumn);
            } else {
                checkUniqueOrPrimaryKeyIntegrityConditionForUpdateWithWhere(table, tableName, constraintTypeName, constraintName, uniqueAttributeName, settingValue, databasesForColumn, whereClause);
            }
        }
    }

    private void checkUniqueOrPrimaryKeyIntegrityConditionForUpdateWithOutWhere(String tableName, String constraintTypeName, String constraintName, List<Integer> databasesForUniqueAttribute) throws SemanticValidationException, FedException {
        // computedCount ist valid when he is 1 or 0
        int computedCount = 0;

        try {
            String query = String.format("SELECT Count(*) FROM %s", tableName);
            HashMap<Integer, Connection> connections = fedConnection.getConnections();

            for (int dbID : databasesForUniqueAttribute) {
                Connection connection = connections.get(dbID);
                try (java.sql.Statement statement = connection.createStatement()) {
                    statement.setMaxRows(2);
                    Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), dbID, query));
                    try (ResultSet result = statement.executeQuery(query)) {
                        while (result.next()) {
                            computedCount += result.getInt(1);
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an update - Querying underlying databases failed.%n%s", this.getClass().getSimpleName(), constraintTypeName, constraintName, ex.getMessage()), ex);
        }

        if (computedCount > 1) {
            throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an update - This update violates the %s constraint \"%s\".", this.getClass().getSimpleName(), constraintTypeName, constraintName, constraintTypeName.toLowerCase(), constraintName));
        }
    }

    private void checkUniqueOrPrimaryKeyIntegrityConditionForUpdateWithWhere(Table table, String tableName, String constraintTypeName, String constraintName, String uniqueAttributeName, String settingValue, List<Integer> databasesForUniqueColumn, WhereClause whereClause) throws SemanticValidationException, FedException {
        HashMap<Integer, Connection> connections = fedConnection.getConnections();

        BinaryExpression binaryExpression = whereClause.getBinaryExpression();
        if (!(whereClause.getBinaryExpression().getLeftOperand() instanceof AttributeIdentifier && binaryExpression.getRightOperand() instanceof Literal)) {
            throw new SemanticValidationException(String.format("%s: Where clause containts a not supported binary expression: %s %s %s.", this.getClass().getSimpleName(),
                    binaryExpression.getLeftOperand().getClass().getName(), binaryExpression.getOperatorName(), binaryExpression.getRightOperand().getClass().getName()));
        }

        String whereQuery = "";
        String leftAttributeName = ((AttributeIdentifier) whereClause.getBinaryExpression().getLeftOperand()).getIdentifier();
        if (binaryExpression.getRightOperand() instanceof StringLiteral) {
            whereQuery = String.format("SELECT * FROM %s WHERE %s = '%s'", tableName, leftAttributeName, ((StringLiteral) binaryExpression.getRightOperand()).getIdentifier());
        } else if (binaryExpression.getRightOperand() instanceof IntegerLiteral) {
            whereQuery = String.format("SELECT * FROM %s WHERE %s = %s", tableName, leftAttributeName, ((IntegerLiteral) binaryExpression.getRightOperand()).getIdentifier());
        } else if (binaryExpression.getRightOperand() instanceof NullLiteral) {
            throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an insert - Null values as primary key are not allowed.", this.getClass().getSimpleName(), constraintTypeName, constraintName));
        } else {
            throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an insert - The value has a unknown type.", this.getClass().getSimpleName(), constraintTypeName, constraintName));
        }

        Constraint primaryKeyConstraint = table.getPrimaryKeyConstraints().get(0);
        String primaryAttributeName = primaryKeyConstraint.getAttributeName();

        int rowsThatWillbeUpdated = 0;
        String primaryKeyFromFirstRowThatWillBeUpadated = "";
        try {
            for (int dbID : table.getDatabasesForColumn(leftAttributeName)) {
                Connection connection = connections.get(dbID);
                try (java.sql.Statement statement = connection.createStatement()) {
                    statement.setMaxRows(2);
                    Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), dbID, whereQuery));
                    try (ResultSet result = statement.executeQuery(whereQuery)) {
                        while (result.next()) {
                            rowsThatWillbeUpdated++;
                            if (rowsThatWillbeUpdated == 1) {
                                primaryKeyFromFirstRowThatWillBeUpadated = result.getString(primaryAttributeName);
                            }
                            if (rowsThatWillbeUpdated > 1) {
                                break;
                            }
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an update - Querying underlying databases failed.%n%s", this.getClass().getSimpleName(), constraintTypeName, constraintName, ex.getMessage()), ex);
        }

        if (rowsThatWillbeUpdated > 1) {
            throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an update - This update violates the %s constraint \"%s\".", this.getClass().getSimpleName(), constraintTypeName, constraintName, constraintTypeName.toLowerCase(), constraintName));
        }

        if (rowsThatWillbeUpdated == 1) {
            Column uniqueColumn = table.getColumn(uniqueAttributeName);
            String uniqueAttributeType = uniqueColumn.getType();

            String checkQuery = "";
            if (uniqueAttributeType.startsWith("VARCHAR")) {
                checkQuery = String.format("SELECT * FROM %s WHERE %s = '%s'", tableName, uniqueAttributeName, settingValue);
            } else if (uniqueAttributeType.equals(PrimitiveType.Primitive.INTEGER.name())) {
                checkQuery = String.format("SELECT * FROM %s WHERE %s = %s", tableName, uniqueAttributeName, settingValue);
            } else {
                throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an insert - The \"%s\" attribute \"%s\" has a unknown type.", this.getClass().getSimpleName(), constraintTypeName, constraintName, constraintTypeName.toLowerCase(), uniqueAttributeName));
            }

            try {
                for (int dbID : databasesForUniqueColumn) {
                    Connection connection = connections.get(dbID);
                    try (java.sql.Statement statement = connection.createStatement()) {
                        statement.setMaxRows(1);
                        Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), dbID, checkQuery));
                        try (ResultSet result = statement.executeQuery(checkQuery)) {
                            while (result.next()) {
                                if (!result.getString(primaryAttributeName).equals(primaryKeyFromFirstRowThatWillBeUpadated)) {
                                    throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an update - This update violates the %s constraint \"%s\".", this.getClass().getSimpleName(), constraintTypeName, constraintName, constraintTypeName.toLowerCase(), constraintName));
                                }
                            }
                        }
                    }
                }
            } catch (SQLException ex) {
                throw new SemanticValidationException(String.format("%s: %s constraint \"%s\" validation for an update - Querying underlying databases failed.%n%s", this.getClass().getSimpleName(), constraintTypeName, constraintName, ex.getMessage()), ex);
            }
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Check Check Integrity Conditions Helper Functions">
    private void checkInsertAttributeAttributeComparisonIntegrityCheck(Constraint constraint, Table table, HashMap<String, String> attributeNameValueMapping) throws SemanticValidationException {
        if (constraint instanceof BinaryConstraint) {
            BinaryConstraint binaryConstraint = (BinaryConstraint) constraint;
            if (binaryConstraint.getBinaryType() == BinaryType.attribute_attribute) {
                String constraintName = constraint.getName();
                String constraintLeftAttributeName = binaryConstraint.getLeftColumnName();
                String leftOperandType = binaryConstraint.getLeftColumnType();
                String constraintRightAttributeName = binaryConstraint.getRightLiteralValue();
                String rightOperandType = binaryConstraint.getRightLiteralType();

                if (!((leftOperandType.equals(PrimitiveType.Primitive.INTEGER.name()) && rightOperandType.equals(PrimitiveType.Primitive.INTEGER.name()))
                        || (leftOperandType.startsWith("VARCHAR") && rightOperandType.startsWith("VARCHAR")))) {
                    throw new SemanticValidationException(String.format("%s: Check constraint \"%s\" validation - Can not validate a expression which have two different attribute/value types.", this.getClass().getSimpleName(), constraintName));
                }

                boolean isExpressionValid = false;
                String globalLeftValue = "";
                String globalRightValue = "";

                if (leftOperandType.startsWith("VARCHAR")) {
                    globalLeftValue = attributeNameValueMapping.get(constraintLeftAttributeName);
                    globalRightValue = attributeNameValueMapping.get(constraintRightAttributeName);

                    // Zahlen < Buchstaben < Kleinbuchstaben
                    switch (binaryConstraint.getOperator()) {
                        case equals:
                            isExpressionValid = globalLeftValue.equals(globalRightValue);
                            break;
                        case greater:
                            isExpressionValid = globalLeftValue.compareTo(globalRightValue) > 0;
                            break;
                        case lower:
                            isExpressionValid = globalLeftValue.compareTo(globalRightValue) < 0;
                            break;
                        case greaterequal:
                            isExpressionValid = globalLeftValue.compareTo(globalRightValue) > 0 || globalLeftValue.equals(globalRightValue);
                            break;
                        case lowerequal:
                            isExpressionValid = globalLeftValue.compareTo(globalRightValue) < 0 || globalLeftValue.equals(globalRightValue);
                            break;
                        case notequals:
                            isExpressionValid = !globalLeftValue.equals(globalRightValue);
                            break;
                    }

                } else if (leftOperandType.equals(PrimitiveType.Primitive.INTEGER.name())) {
                    int leftValue = Integer.parseInt(attributeNameValueMapping.get(constraintLeftAttributeName));
                    int rightValue = Integer.parseInt(attributeNameValueMapping.get(constraintRightAttributeName));
                    globalLeftValue = Integer.toString(leftValue);
                    globalRightValue = Integer.toString(rightValue);

                    switch (binaryConstraint.getOperator()) {
                        case equals:
                            isExpressionValid = leftValue == rightValue;
                            break;
                        case greater:
                            isExpressionValid = leftValue > rightValue;
                            break;
                        case lower:
                            isExpressionValid = leftValue < rightValue;
                            break;
                        case greaterequal:
                            isExpressionValid = leftValue >= rightValue;
                            break;
                        case lowerequal:
                            isExpressionValid = leftValue <= rightValue;
                            break;
                        case notequals:
                            isExpressionValid = leftValue != rightValue;
                            break;
                    }
                }

                if (!isExpressionValid) {
                    throw new SemanticValidationException(String.format("%s: Check constraint \"%s\" validation - The binary expression value \"%s\" is not %s then \"%s\".", this.getClass().getSimpleName(), constraintName, globalLeftValue, binaryConstraint.getOperator().name(), globalRightValue));
                }
            }
        }
    }

    private void checkUpdateWithoutWehreAttributeAttributeComparisonIntegrityCheck(Constraint constraint, Table table, HashMap<String, String> attributeNameValueMapping) throws SemanticValidationException {
//        if (constraint instanceof BinaryConstraint) {
//            BinaryConstraint binaryConstraint = (BinaryConstraint) constraint;
//            String constraintLeftAttributeName = binaryConstraint.getLeftColumnName();
//            String constraintRightAttributeName = binaryConstraint.getRightLiteralValue();
//
//            if (binaryConstraint.getBinaryType() == BinaryType.attribute_attribute && (attributeNameValueMapping.containsKey(constraintLeftAttributeName) || attributeNameValueMapping.containsKey(constraintRightAttributeName))) {
//                String constraintName = constraint.getName();
//                String leftOperandType = binaryConstraint.getLeftColumnType();
//                String rightOperandType = binaryConstraint.getRightLiteralType();
//
//                if (!((leftOperandType.equals(PrimitiveType.Primitive.INTEGER.name()) && rightOperandType.equals(PrimitiveType.Primitive.INTEGER.name()))
//                        || (leftOperandType.startsWith("VARCHAR") && rightOperandType.startsWith("VARCHAR")))) {
//                    throw new SemanticValidationException(String.format("Check constraint \"%s\" validation: Can not validate a expression which have two different attribute/value types.", constraintName));
//                }
//                
//                String checkingAttribute = "";
//
//                String query = "";
//
//                boolean allPossibleRowsCanBeUpdated = false;
//
//                try {
//                    HashMap<Integer, Connection> connections = fedConnection.getConnections();
//                    List<Integer> databasesForColumn = table.getDatabasesForColumn(checkingAttribute);
//
//                    if (databasesForColumn.isEmpty()) {
//                        throw new SemanticValidationException(String.format("Check constraint \"%s\" validation: The check attribute \"%s\" could not be assigned to any database.", constraintName, refAttributeName));
//                    }
//
//                    for (int dbID : databasesForColumn) {
//                        Connection connection = connections.get(dbID);
//                      Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), dbID, query));
//                        try (java.sql.Statement statement = connection.createStatement();
//                                ResultSet result = statement.executeQuery(query)) {
//                            if (result.next()) {
//                                int count = result.getInt(1);
//                                if (count > 0) {
//                                    allPossibleRowsCanBeUpdated |= true;
//                                }
//                                if (allPossibleRowsCanBeUpdated) {
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                } catch (SQLException ex) {
//                    throw new SemanticValidationException(String.format("Check constraint \"%s\" validation: Querying underlying databases failed.", constraintName), ex);
//                }
//
//                if (!allPossibleRowsCanBeUpdated) {
//                    throw new SemanticValidationException(String.format("Posible constraint \"%s\" validation: The foreign key constraint value \"%s\" is unknown.", constraintName, attributeValue));
//                }
//            }
//        }
    }

    // </editor-fold>
    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Check Attribute Helper Functions">
    protected void checkFullQualifiedAttributes(HashMap<String, Table> tableInfos, List<FullyQualifiedAttributeIdentifier> fQAttributes, String optionalExceptionPrefix) throws SemanticValidationException {
        if (fQAttributes != null) {
            for (FullyQualifiedAttributeIdentifier fQAttribute : fQAttributes) {
                checkSingleFullQualifiedAttribute(tableInfos, fQAttribute, optionalExceptionPrefix);
            }
        }
    }

    protected void checkSingleFullQualifiedAttribute(HashMap<String, Table> tableInfos, FullyQualifiedAttributeIdentifier fQAttribute, String optionalExceptionPrefix) throws SemanticValidationException {
        String tableName = fQAttribute.getIdentifier();
        String attributeName = fQAttribute.getAttributeIdentifier().getIdentifier();

        Table table = tableInfos.get(tableName);

        if (table == null) {
            throw new SemanticValidationException(String.format("%s: %sThe table name \"%s\" from the full qualified attribute \"%s.%s\" is unknown.", this.getClass().getSimpleName(), optionalExceptionPrefix, tableName, tableName, attributeName));
        }

        Column column = table.getColumn(attributeName);
        if (column == null) {
            throw new SemanticValidationException(String.format("%s: %sThe attribute \"%s\" does not exists in the table \"%s\".", this.getClass().getSimpleName(), optionalExceptionPrefix, attributeName, tableName));
        }
    }
    // </editor-fold>
}

// </editor-fold>
