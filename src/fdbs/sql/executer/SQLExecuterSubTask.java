package fdbs.sql.executer;

import fdbs.sql.FedException;
import fdbs.util.DatabaseCursorChecker;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLExecuterSubTask implements AutoCloseable {

    private final String query;
    private ResultSet queryResultSet;
    private int updateResult;
    private boolean isUpdate;
    private boolean isClosed;

    public SQLExecuterSubTask(String query, boolean isUpdate) {
        this.query = query;
        this.queryResultSet = null;
        this.isUpdate = isUpdate;
    }

    public String getQuery() throws FedException {
        checkIsClosed();
        return query;
    }

    public boolean isUpdate() throws FedException {
        checkIsClosed();
        return this.isUpdate;
    }

    public int getUpdateResult() throws FedException {
        checkIsClosed();
        return updateResult;
    }

    public void setUpdateResult(int updateResult) throws FedException {
        checkIsClosed();
        this.updateResult = updateResult;
    }

    public ResultSet getQueryResultSet() throws FedException {
        checkIsClosed();
        return queryResultSet;
    }

    public void setQueryResultSet(ResultSet resultSet) throws FedException {
        checkIsClosed();
        this.queryResultSet = resultSet;
    }

    @Override
    public void close() throws FedException {
        checkIsClosed();
        if (queryResultSet != null) {
            try (Statement statement = queryResultSet.getStatement()) {
                if (!queryResultSet.isClosed()) {
                    queryResultSet.close();
                    DatabaseCursorChecker.closeResultSet(this.getClass());
                }
            } catch (SQLException ex) {
                throw new FedException(String.format("%s: An error occurred while closing a local result set.%n%s",
                        this.getClass().getSimpleName(), ex.getMessage()), ex);
            } finally {
                DatabaseCursorChecker.closeStatement(this.getClass());
            }
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    private void checkIsClosed() throws FedException {
        if (isClosed) {
            throw new FedException(String.format("%s: The sql executer sub task is closed.", this.getClass().getSimpleName()));
        }
    }
}
