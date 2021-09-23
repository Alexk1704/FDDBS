package fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterSubTask;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.Constraint;
import fdbs.sql.meta.ConstraintType;
import fdbs.sql.meta.HorizontalPartition;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.statement.UpdateStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UpdateStatementResolver extends StatementResolverManager {

    private String tableName;
    private UpdateStatement statement;
    private String attribute;
    private String compareOperator;
    private BinaryExpression whereClause;
    private ArrayList<HorizontalPartition> hList;

    private HashMap<String, Column> vMap;

    private String primaryKeyAttributeName = "";
    private boolean primaryKeyIsInt;
    private String newValueFormatted;
    private String newValueRaw;
    private String whereClauseString;

    public UpdateStatementResolver(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public SQLExecuterTask resolveStatement(AST tree) throws FedException {
        statement = (UpdateStatement) tree.getRoot();
        tableName = statement.getTableIdentifier().getIdentifier();
        SQLExecuterTask executerTask;

        try {
            executerTask = updateStatement();
        } catch (StatementResolverException | SQLException | FedException ex) {
            throw new FedException(String.format("%s: An error occurred while resolving update statement.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }

        return executerTask;
    }

    private SQLExecuterTask updateStatement() throws StatementResolverException, SQLException, FedException {
        hList = metadataManager.getHorizontalPartition(tableName);
        vMap = metadataManager.getVerticalPartition(tableName);

        newValueFormatted = newValueRaw = statement.getConstant().getIdentifier();

        if (statement.getConstant() instanceof IntegerLiteral) {
            newValueFormatted = String.format("%s", newValueFormatted);
        } else if (statement.getConstant() instanceof NullLiteral) {
            newValueFormatted = "null";
        } else {
            newValueFormatted = String.format("'%s'", newValueFormatted);
        }

        attribute = statement.getAttributeIdentifier().getIdentifier();

        if (statement.getWhereClause() != null) {
            whereClause = statement.getWhereClause().getBinaryExpression();
        }

        if (whereClause != null) {
            compareOperator = whereClause.getSqlOperatorName();
            if (whereClause.getRightOperand() instanceof IntegerLiteral) {
                whereClauseString = String.format("WHERE %s %s %s", whereClause.getLeftOperand().getIdentifier(), compareOperator, whereClause.getRightOperand().getIdentifier());
            } else if (whereClause.getRightOperand() instanceof NullLiteral) {
                whereClauseString = String.format("WHERE %s %s null", whereClause.getLeftOperand().getIdentifier(), compareOperator);
            } else {
                whereClauseString = String.format("WHERE %s %s '%s'", whereClause.getLeftOperand().getIdentifier(), compareOperator, whereClause.getRightOperand().getIdentifier());
            }
        } else {
            whereClauseString = "";
        }
        List<Constraint> cons = metadataManager.getConstraintsByTable(tableName);
        for (Constraint con : cons) {
            if (con.getType() == ConstraintType.PrimaryKey) {
                primaryKeyAttributeName = con.getAttributeName();
                primaryKeyIsInt = metadataManager.getColumnType(tableName, primaryKeyAttributeName).startsWith("VARCHAR") == false;
                break;
            }
        }

        if ("".equals(primaryKeyAttributeName)) {
            throw new StatementResolverException(String.format("%s: Could not determine PrimaryKey for %s.", this.getClass().getSimpleName(), tableName));
        }

        if (hList.size() > 0) {
            return handleHorizontal();
        } else if (vMap.size() > 0) {
            return handleVertical();
        } else {
            return handleNormal();
        }
    }

    private SQLExecuterTask handleHorizontal() throws FedException, SQLException {
        // updaten von Zeilen in jeder der drei Datenbanken, auf die die Bedingung zutrifft
        // falls das horizontal verteilte Attribut aktualisiert wird, muss die Zeile ggf in eine 
        // andere DB verschoben werden.
        String federatedAttribute = hList.get(0).getAttributeName();
        SQLExecuterTask task = new SQLExecuterTask();

        //Fall 1: federatedAttribute wird aktualisiert.
        String query = "";
        if (attribute.equals(federatedAttribute)) {
            SQLExecuter sqlExecuter = new SQLExecuter(fedConnection);
            query = String.format("SELECT * FROM %s %s", tableName, whereClauseString);
            task.addSubTask(query, false, 1);
            task.addSubTask(query, false, 2);
            task.addSubTask(query, false, 3);
            sqlExecuter.executeTask(task);
            SQLExecuterTask newtask = new SQLExecuterTask();
            String primaryKeyValueFormatted = "";
            for (int i = 1; i <= hList.size(); i++) {
                ResultSet results = task.getDBStatements(i).get(0).getQueryResultSet();
                while (results.next()) {
                    primaryKeyValueFormatted = this.primaryKeyIsInt ? String.format("%s", results.getString(primaryKeyAttributeName))
                            : String.format("'%s'", results.getString(primaryKeyAttributeName));
                    if (hList.get(i - 1).compareValue(newValueRaw)) {
                        //keep in this database because constant is still in horizontal "range"
                        query = String.format("UPDATE %s SET %s = %s WHERE %s = %s", tableName, attribute, newValueFormatted, primaryKeyAttributeName, primaryKeyValueFormatted);
                        newtask.addSubTask(query, true, i);
                    } else {
                        //delete and insert in other database
                        query = String.format("DELETE FROM %s WHERE %s = %s", tableName, primaryKeyAttributeName, primaryKeyValueFormatted);

                        newtask.addSubTask(query, true, i);

                        query = String.format("INSERT INTO %s VALUES (", tableName);

                        List<Column> cols = metadataManager.getColumns(tableName);
                        for (Column col : cols) {
                            if (col.getAttributeName().equals(attribute)) {
                                query += String.format(" %s, ", newValueFormatted);
                            } else {
                                String currentValue = results.getString(col.getAttributeName());
                                if (col.getType().startsWith("VARCHAR") && !results.wasNull()) {
                                    query += String.format(" '%s', ", currentValue);
                                } else {
                                    query += String.format(" %s, ", currentValue);
                                }
                            }
                        }

                        query = query.substring(0, query.length() - 2) + ")";

                        for (HorizontalPartition h : hList) {
                            if (h.compareValue(newValueRaw) == true) {
                                newtask.addSubTask(query, true, h.getDatabase());
                                break;
                            }
                        }
                    }

                }
            }
            return newtask;

        } //Fall 2: federatedAttribute selbst wird nicht aktualisiert.
        else {
            query = String.format("UPDATE %s SET %s = %s %s", tableName, attribute, newValueFormatted, whereClauseString);
            task.addSubTask(query, true, 1);
            task.addSubTask(query, true, 2);
            task.addSubTask(query, true, 3);

            return task;
        }
    }

    private SQLExecuterTask handleVertical() throws SQLException, FedException {
        int db = metadataManager.getColumn(tableName, attribute).getDatabase();
        String query = "";

        if (whereClauseString == null || whereClauseString.isEmpty()) {
            query = String.format("UPDATE %s SET %s = %s", tableName, attribute, newValueFormatted);
            SQLExecuterTask task = new SQLExecuterTask();
            task.addSubTask(query, true, 1);

            return task;
        } else {
            int whereDB = metadataManager.getColumn(tableName, whereClause.getLeftOperand().getIdentifier()).getDatabase();

            if (whereDB == db) {
                query = String.format("UPDATE %s SET %s = %s %s", tableName, attribute, newValueFormatted, whereClauseString);
                SQLExecuterTask task = new SQLExecuterTask();
                task.addSubTask(query, true, 1);

                return task;
            } else {
                // have to select all rows that match the update condition
                SQLExecuter sqlExecuter = new SQLExecuter(fedConnection);
                String selectQuery = String.format("SELECT %s FROM %s %s", primaryKeyAttributeName, tableName, this.whereClauseString);
                SQLExecuterTask task = new SQLExecuterTask();
                task.addSubTask(new SQLExecuterSubTask(selectQuery, false), whereDB);
                ResultSet results;

                sqlExecuter.executeTask(task);
                results = task.getDBStatements(whereDB).get(0).getQueryResultSet();

                task = new SQLExecuterTask();
                if (results != null) {
                    while (results.next()) {
                        query = "UPDATE %s SET %s = %s WHERE %s = " + (primaryKeyIsInt ? "%s" : "'%s'");
                        String qs = String.format(query, tableName, attribute, newValueFormatted, primaryKeyAttributeName, results.getString(primaryKeyAttributeName));
                        SQLExecuterSubTask t = new SQLExecuterSubTask(qs, true);
                        task.addSubTask(t, db);
                    }
                }

                return task;
            }
        }

    }

    private SQLExecuterTask handleNormal() throws FedException {
        SQLExecuterTask task = new SQLExecuterTask();
        String query = String.format("UPDATE %s SET %s = %s %s ", tableName, attribute, newValueFormatted, whereClauseString);
        task.addSubTask(query, true, 1);
        return task;
    }

}
