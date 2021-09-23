package fdbs.sql.executer;

import fdbs.sql.FedException;
import java.util.ArrayList;
import java.util.List;

public class SQLExecuterTask implements AutoCloseable {

    private final List<SQLExecuterSubTask> db1Statements;
    private final List<SQLExecuterSubTask> db2Statements;
    private final List<SQLExecuterSubTask> db3Statements;
    private boolean isClosed;

    public SQLExecuterTask(List<SQLExecuterSubTask> db1Statements, List<SQLExecuterSubTask> db2Statements, List<SQLExecuterSubTask> db3statements) {
        this.db1Statements = db1Statements;
        this.db2Statements = db2Statements;
        this.db3Statements = db3statements;
    }

    public SQLExecuterTask() {
        this.db1Statements = new ArrayList<>();
        this.db2Statements = new ArrayList<>();
        this.db3Statements = new ArrayList<>();
    }

    public void addSubTask(String sqlQuery, boolean isUpdate, int databaseId) throws FedException {
        checkIsClosed();
        SQLExecuterSubTask subtask = new SQLExecuterSubTask(sqlQuery, isUpdate);
        this.addSubTask(subtask, databaseId);
    }

    public void addSubTask(SQLExecuterSubTask subtask, int databaseId) throws FedException {
        checkIsClosed();
        switch (databaseId) {
            case 1:
                this.db1Statements.add(subtask);
                break;
            case 2:
                this.db2Statements.add(subtask);
                break;
            case 3:
                this.db3Statements.add(subtask);
                break;
            default:
                throw new FedException(String.format("%s: Invalid database id specified.", this.getClass().getSimpleName()));

        }
    }

    public List<SQLExecuterSubTask> getDb1Statements() throws FedException {
        checkIsClosed();
        return db1Statements;
    }

    public List<SQLExecuterSubTask> getDb2Statements() throws FedException {
        checkIsClosed();
        return db2Statements;
    }

    public List<SQLExecuterSubTask> getDb3Statements() throws FedException {
        checkIsClosed();
        return db3Statements;
    }

    public List<SQLExecuterSubTask> getDBStatements(int db) throws FedException {
        checkIsClosed();

        switch (db) {
            case 1:
                return getDb1Statements();
            case 2:
                return getDb2Statements();
            case 3:
                return getDb3Statements();
            default:
                throw new FedException(String.format("%s: Unknow database id", this.getClass().getSimpleName()));
        }
    }

    @Override
    public void close() throws FedException {
        checkIsClosed();

        List<FedException> occuredExceptions = new ArrayList<>();

        getDb1Statements().forEach((subTask) -> {
            try {
                if (!subTask.isClosed()) {
                    subTask.close();
                }
            } catch (FedException ex) {
                occuredExceptions.add(ex);
            }
        });

        getDb2Statements().forEach((subTask) -> {
            try {
                if (!subTask.isClosed()) {
                    subTask.close();
                }
            } catch (FedException ex) {
                occuredExceptions.add(ex);
            }
        });

        getDb3Statements().forEach((subTask) -> {
            try {
                if (!subTask.isClosed()) {
                    subTask.close();
                }
            } catch (FedException ex) {
                occuredExceptions.add(ex);
            }
        });

        if (occuredExceptions.size() > 0) {
            FedException fedException = new FedException(String.format("%s:  An error occurred while close tasks result sets.%n%s", this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));

            occuredExceptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            throw fedException;
        }
    }

    private void checkIsClosed() throws FedException {
        if (isClosed) {
            throw new FedException(String.format("%s: The sql executer sub task is closed.", this.getClass().getSimpleName()));
        }
    }
}
