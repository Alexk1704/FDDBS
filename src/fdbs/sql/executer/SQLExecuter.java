package fdbs.sql.executer;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.util.DatabaseCursorChecker;
import fdbs.util.logger.Logger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class SQLExecuter {

    private final FedConnection fedConnection;

    public SQLExecuter(FedConnection fedConnection) {
        this.fedConnection = fedConnection;
    }

    public void executeTask(SQLExecuterTask task) throws SQLException, FedException {
        HashMap<Integer, Connection> connections = fedConnection.getConnections();
        Connection conDb1 = connections.get(1);
        Connection conDb2 = connections.get(2);
        Connection conDb3 = connections.get(3);

        for (SQLExecuterSubTask subtask : task.getDb1Statements()) {
            Statement statement = conDb1.createStatement();
            String query = subtask.getQuery();
            if (subtask.isUpdate()) {
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", query));
                subtask.setUpdateResult(statement.executeUpdate(query));
                statement.close();
            } else {
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", query));
                statement.executeQuery(query);
                subtask.setQueryResultSet(statement.getResultSet());
                DatabaseCursorChecker.openStatement(this.getClass());
                DatabaseCursorChecker.openResultSet(this.getClass());
            }
        }

        for (SQLExecuterSubTask subtask : task.getDb2Statements()) {
            Statement statement = conDb2.createStatement();
            String query = subtask.getQuery();
            if (subtask.isUpdate()) {
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "2", query));
                subtask.setUpdateResult(statement.executeUpdate(query));
                statement.close();
            } else {
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "2", query));
                statement.executeQuery(query);
                subtask.setQueryResultSet(statement.getResultSet());
                DatabaseCursorChecker.openStatement(this.getClass());
                DatabaseCursorChecker.openResultSet(this.getClass());
            }
        }

        for (SQLExecuterSubTask subtask : task.getDb3Statements()) {
            Statement statement = conDb3.createStatement();
            String query = subtask.getQuery();
            if (subtask.isUpdate()) {
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "3", query));
                subtask.setUpdateResult(statement.executeUpdate(query));
                statement.close();
            } else {
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "3", query));
                statement.executeQuery(subtask.getQuery());
                subtask.setQueryResultSet(statement.getResultSet());
                DatabaseCursorChecker.openStatement(this.getClass());
                DatabaseCursorChecker.openResultSet(this.getClass());
            }
        }
    }
}
