package fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.HorizontalPartition;
import fdbs.sql.meta.PartitionType;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.statement.DeleteStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteStatementResolver extends StatementResolverManager {

    private DeleteStatement statement;
    private String tableName;
    private Table table;
    private String whereAttribute;
    private String whereString;

    public DeleteStatementResolver(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public SQLExecuterTask resolveStatement(AST tree) throws FedException {
        statement = (DeleteStatement) tree.getRoot();
        tableName = statement.getTableIdentifier().getIdentifier();
        table = metadataManager.getTable(tableName);
        whereString = "";
        whereAttribute = "";
        if (statement.getWhereClause() != null) {
            BinaryExpression whereBinaryExpression = statement.getWhereClause().getBinaryExpression();
            whereAttribute = whereBinaryExpression.getLeftOperand().getIdentifier();
            if (whereBinaryExpression.getRightOperand() instanceof NullLiteral) {
                whereString = String.format("WHERE %s %s null", whereAttribute,
                        whereBinaryExpression.getSqlOperatorName());
            } else if (whereBinaryExpression.getRightOperand() instanceof IntegerLiteral) {
                whereString = String.format("WHERE %s %s %s", whereAttribute,
                        whereBinaryExpression.getSqlOperatorName(), whereBinaryExpression.getRightOperand().getIdentifier());
            } else {
                whereString = String.format("WHERE %s %s '%s'", whereAttribute,
                        whereBinaryExpression.getSqlOperatorName(), whereBinaryExpression.getRightOperand().getIdentifier());
            }
        }

        SQLExecuterTask executerTask;
        try {
            executerTask = deleteStatement();
        } catch (SQLException | FedException ex) {
            throw new FedException(String.format("%s: An error occurred while resolving delete statement.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return executerTask;
    }

    private SQLExecuterTask deleteStatement() throws SQLException, FedException {
        if (table.getPartitionType() == PartitionType.Horizontal) {
            return handleHorizontal();
        } else if (table.getPartitionType() == PartitionType.Vertical) {
            return handleVertical();
        } else {
            return handleNormal();
        }
    }

    private SQLExecuterTask handleHorizontal() throws FedException {
        SQLExecuterTask task = new SQLExecuterTask();
        for (HorizontalPartition horizontalpartition : table.getHorizontalPartitions()) {
            String query = String.format("DELETE FROM %s %s", tableName, whereString);
            task.addSubTask(query, true, horizontalpartition.getDatabase());
        }
        return task;
    }

    private SQLExecuterTask handleVertical() throws SQLException, FedException {
        SQLExecuterTask returnTask = new SQLExecuterTask();

        if (whereString == null || whereString.isEmpty()) {
            int last = -1;
            for (Column col : table.getColumns()) {
                if (col.getDatabase() != last) {
                    last = col.getDatabase();
                    returnTask.addSubTask(String.format("DELETE FROM %s", tableName), true, last);
                }
            }
        } else {
            SQLExecuter executer = new SQLExecuter(fedConnection);
            String query;
            String primaryKeyName = table.getPrimaryKeyConstraints().get(0).getAttributeName();
            boolean isIntPK = table.getColumn(primaryKeyName).getType().startsWith("VARCHAR") == false;
            SQLExecuterTask selectTask = new SQLExecuterTask();
            Column c = table.getColumn(whereAttribute);
            query = String.format("SELECT %s FROM %s %s ", primaryKeyName, tableName, whereString);
            selectTask.addSubTask(query, false, c.getDatabase());

            executer.executeTask(selectTask);
            ResultSet results = selectTask.getDBStatements(c.getDatabase()).get(0).getQueryResultSet();
            while (results.next()) {
                if (isIntPK) {
                    query = String.format("DELETE FROM %s WHERE %s = %s", tableName, primaryKeyName, results.getString(primaryKeyName));
                } else {
                    query = String.format("DELETE FROM %s WHERE %s = '%s'", tableName, primaryKeyName, results.getString(primaryKeyName));
                }

                int dbid = -1;
                for (Column col : table.getColumns()) {
                    if (col.getDatabase() != dbid) {
                        dbid = col.getDatabase();
                        returnTask.addSubTask(query, true, dbid);
                    }
                }
            }
        }
        return returnTask;
    }

    private SQLExecuterTask handleNormal() throws FedException {
        String query = String.format("DELETE FROM %s %s", tableName, whereString);
        SQLExecuterTask task = new SQLExecuterTask();
        task.addSubTask(query, true, 1);
        return task;
    }

}
