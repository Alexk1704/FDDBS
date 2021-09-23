package fdbs.sql.fedresultset.select.countalltablestatement;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.statement.select.SelectCountAllTableStatement;
import fdbs.util.DatabaseCursorChecker;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public final class SelectCountAllTableStatementHorizontalResultSet extends FedResultSet {

    private Table table = null;
    private HashMap<Integer, ResultSet> localHorizontalResultSets;
    private ResultSetMetaData rs1Metdata = null;

    public SelectCountAllTableStatementHorizontalResultSet(FedConnection fedConnection,
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
        boolean next = false;
        for (ResultSet rs : localHorizontalResultSets.values()) {
            boolean localNext = rs.next();
            if (next == true && localNext == false) {
                throw new FedException(String.format("%s: Unexpceted local results. Not all local result sets have a result.", this.getClass().getSimpleName()));
            }
            next = localNext;
        }

        return next;
    }

    @Override
    protected String getPrivateString(int columnIndex) throws Exception {
        return Integer.toString(getResultSetSum(columnIndex));
    }

    @Override
    protected int getPrivateInt(int columnIndex) throws Exception {
        return getResultSetSum(columnIndex);
    }

    @Override
    protected int getPrivateColumnCount() throws Exception {
        return rs1Metdata.getColumnCount(); // All local result sets have the same row count
    }

    @Override
    protected String getPrivateColumnName(int columnIndex) throws Exception {
        return rs1Metdata.getColumnName(columnIndex); // All local result sets have the columns
    }

    @Override
    protected int getPrivateColumnType(int columnIndex) throws Exception {
        return rs1Metdata.getColumnType(columnIndex); // All local result sets have the column typs
    }

    @Override
    protected FedException closeLocalResultSets() {
        FedException fedException = null;
        List<SQLException> occuredExceptions = new ArrayList<>();

        if (localHorizontalResultSets != null) {
            for (ResultSet rs : localHorizontalResultSets.values()) {
                try {
                    if (!rs.isClosed()) {
                        try (Statement resultSetStatement = rs.getStatement()) {
                            rs.close();
                            DatabaseCursorChecker.closeResultSet(this.getClass());
                        } finally {
                            DatabaseCursorChecker.closeStatement(this.getClass());
                        }
                    }
                } catch (SQLException ex) {
                    occuredExceptions.add(ex);
                }
            }
        }

        rs1Metdata = null;

        if (occuredExceptions.size() > 0) {
            fedException = new FedException(String.format("%s: An error occurred while closing local result sets.%n%s", this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));
            for (Exception exception : occuredExceptions) {
                fedException.addSuppressed(exception);
            }
        }

        return fedException;
    }

    @Override
    protected void resolveStatement() throws FedException {
        String tableName = ((SelectCountAllTableStatement) statement).getTableIdentifier().getIdentifier();
        table = metadataManager.getTable(tableName);
        String stmt = String.format("SELECT COUNT(*) FROM %s", tableName);

        localHorizontalResultSets = getLocalResultSetsHorizontal(table, stmt);
        rs1Metdata = initialiseMetaData(localHorizontalResultSets.get(1));
    }

    private int getResultSetSum(int columnIndex) throws SQLException {
        int sum = 0;

        for (ResultSet rs : localHorizontalResultSets.values()) {
            sum += rs.getInt(columnIndex);
        }

        return sum;
    }
}
