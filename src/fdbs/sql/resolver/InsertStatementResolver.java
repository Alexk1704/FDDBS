package fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.Constraint;
import fdbs.sql.meta.HorizontalPartition;
import fdbs.sql.meta.PartitionType;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.literal.Literal;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.statement.InsertStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class InsertStatementResolver extends StatementResolverManager {

    private InsertStatement statement;
    private String tableName;
    private Table table;
    private ArrayList<String> tableAttributeList;
    private ArrayList<String> statementValueList;
    private HashMap<String, String> columnValues;

    public InsertStatementResolver(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public SQLExecuterTask resolveStatement(AST tree) throws FedException {
        statement = (InsertStatement) tree.getRoot();
        tableName = statement.getTableIdentifier().getIdentifier();
        table = metadataManager.getTable(tableName);
        initializeValueLists(statement.getValues());

        SQLExecuterTask executerTask;
        try {
            executerTask = insertStatement();
        } catch (StatementResolverException | FedException ex) {
            throw new FedException(String.format("%s: An error occurred while resolving insert statement.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return executerTask;
    }

    private void initializeValueLists(List<Literal> literals) {
        statementValueList = new ArrayList<>();
        columnValues = new HashMap<>();
        tableAttributeList = new ArrayList<>();
        ArrayList<Column> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            String columName = column.getAttributeName();
            Literal literal = literals.get(i);
            String value = literal.getIdentifier();
            tableAttributeList.add(columName);
            if (column.getType().startsWith("VARCHAR") && !(literal instanceof NullLiteral)) {
                statementValueList.add(String.format("'%s'", value));
            } else {
                statementValueList.add(value);
            }

            columnValues.put(columName, value);
        }
    }

    private SQLExecuterTask insertStatement() throws FedException, StatementResolverException {
        if (table.getPartitionType() == PartitionType.Horizontal) {
            return handleHorizontal();
        } else if (table.getPartitionType() == PartitionType.Vertical) {
            return handleVertical();
        } else {
            return handleNormal();
        }
    }

    private SQLExecuterTask handleHorizontal() throws FedException, StatementResolverException {
        ArrayList<HorizontalPartition> hList = metadataManager.getHorizontalPartition(tableName);
        String federatedAttribute = hList.get(0).getAttributeName();
        Literal federatedAttributeLiteral = statement.getValues().get(tableAttributeList.indexOf(federatedAttribute));

        int insertingDB = -1;
        if (federatedAttributeLiteral instanceof NullLiteral) {
            insertingDB = 1;
        } else {
            for (HorizontalPartition h : hList) {
                if (h.compareValue(federatedAttributeLiteral.getIdentifier()) == true) {
                    insertingDB = h.getDatabase();
                    break;
                }
            }
        }

        if (insertingDB == -1) {
            throw new StatementResolverException(String.format("%s: Could not determine target database for horizontal partitioning.", this.getClass().getSimpleName()));
        }

        String query = String.format("INSERT INTO %s VALUES (%s)", tableName, String.join(",", statementValueList));
        SQLExecuterTask task = new SQLExecuterTask();
        task.addSubTask(query, true, insertingDB);

        return task;
    }

    private SQLExecuterTask handleNormal() throws FedException {
        String query = String.format("INSERT INTO %s VALUES (%s)", tableName, String.join(",", statementValueList));
        SQLExecuterTask task = new SQLExecuterTask();
        task.addSubTask(query, true, 1);

        return task;
    }

    private SQLExecuterTask handleVertical() throws FedException {
        HashMap<Integer, String> queryValueList = new HashMap<>();
        HashMap<Integer, LinkedHashMap<String, Column>> dbColumns = metadataManager.getTable(tableName).getDatabaseRelatedColumns();

        ArrayList<Constraint> primaryKeyConstraints = table.getPrimaryKeyConstraints();
        String primaryKeyName = primaryKeyConstraints.get(0).getAttributeName();

        for (int dbId : dbColumns.keySet()) {
            LinkedHashMap<String, Column> columns = dbColumns.get(dbId);
            String dbValues = "";
            if (columns != null && !columns.isEmpty()) {
                if (!columns.containsKey(primaryKeyName)) {
                    dbValues += String.format("%s,", statementValueList.get(tableAttributeList.indexOf(primaryKeyName)));
                }

                for (Column column : columns.values()) {
                    dbValues += String.format("%s,", statementValueList.get(tableAttributeList.indexOf(column.getAttributeName())));
                }

                dbValues = dbValues.substring(0, dbValues.length() - 1);
                queryValueList.put(dbId, dbValues);
            }
        }

        String stmtFormat = "INSERT INTO %s VALUES (%s)";
        SQLExecuterTask task = new SQLExecuterTask();

        if (queryValueList.containsKey(1)) {
            String query = String.format(stmtFormat, tableName, queryValueList.get(1));
            task.addSubTask(query, true, 1);
        }
        if (queryValueList.containsKey(2)) {
            String query = String.format(stmtFormat, tableName, queryValueList.get(2));
            task.addSubTask(query, true, 2);
        }
        if (queryValueList.containsKey(3)) {
            String query = String.format(stmtFormat, tableName, queryValueList.get(3));
            task.addSubTask(query, true, 3);
        }

        return task;
    }

}
