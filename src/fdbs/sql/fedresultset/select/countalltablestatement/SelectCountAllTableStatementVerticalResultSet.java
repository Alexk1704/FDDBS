package fdbs.sql.fedresultset.select.countalltablestatement;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.parser.ast.statement.select.SelectCountAllTableStatement;
import fdbs.util.DatabaseCursorChecker;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public final class SelectCountAllTableStatementVerticalResultSet extends FedResultSet {

    private ResultSet localVerticalResultSet = null;
    private ResultSetMetaData rsMetdata = null;

    public SelectCountAllTableStatementVerticalResultSet(FedConnection fedConnection,
            SelectCountAllTableStatement statement) throws FedException {
        super(fedConnection, statement);
        try {
            resolveStatement();
        } catch (FedException ex) {
            closeLocalResultSets();
            throw ex;
        }
    }

    @Override
    protected boolean privateNext() throws Exception {
        return localVerticalResultSet.next();
    }

    @Override
    protected String getPrivateString(int columnIndex) throws Exception {
        return localVerticalResultSet.getString(columnIndex);
    }

    @Override
    protected int getPrivateInt(int columnIndex) throws Exception {
        return localVerticalResultSet.getInt(columnIndex);
    }

    @Override
    protected int getPrivateColumnCount() throws Exception {
        return rsMetdata.getColumnCount();
    }

    @Override
    protected String getPrivateColumnName(int columnIndex) throws Exception {
        return rsMetdata.getColumnName(columnIndex);
    }

    @Override
    protected int getPrivateColumnType(int columnIndex) throws Exception {
        return rsMetdata.getColumnType(columnIndex);
    }

    @Override
    protected FedException closeLocalResultSets() {
        FedException exception = null;
        if (localVerticalResultSet != null) {
            try {
                if (!localVerticalResultSet.isClosed()) {
                    try (Statement resultSetStatement = localVerticalResultSet.getStatement()) {
                        localVerticalResultSet.close();
                        DatabaseCursorChecker.closeResultSet(this.getClass());
                    } finally {
                        DatabaseCursorChecker.closeStatement(this.getClass());
                    }
                }
            } catch (SQLException ex) {
                exception = new FedException(String.format("%s: An error occurred while closing local result sets.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
            }
        }

        rsMetdata = null;

        return exception;
    }

    @Override
    protected void resolveStatement() throws FedException {
        String tableName = ((SelectCountAllTableStatement) statement).getTableIdentifier().getIdentifier();
        String stmt = String.format("SELECT COUNT(*) FROM %s", tableName);

        localVerticalResultSet = getLocalResultSet(stmt, 1);
        rsMetdata = initialiseMetaData(localVerticalResultSet);
    }
}
