package fdbs.sql.fedresultset.select.nogroupstatement;

import java.sql.ResultSet;
import java.util.List;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.statement.Statement;
import fdbs.sql.parser.ast.statement.select.SelectNoGroupStatement;
import fdbs.util.DatabaseCursorChecker;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public final class NoGroupDefaultResultSet extends FedResultSet {

    private SelectNoGroupStatement selectStatement;

    private ResultSetMetaData rsMetdata = null;
    private ResultSet localDefaultResultSet;

    public NoGroupDefaultResultSet(FedConnection fedConnection, Statement statement) throws FedException {
        super(fedConnection, statement);
        try {
            selectStatement = (SelectNoGroupStatement) statement;
            resolveStatement();
        } catch (FedException ex) {
            closeLocalResultSets();
            throw ex;
        }
    }

    @Override
    protected boolean privateNext() throws Exception {
        return localDefaultResultSet.next();
    }

    @Override
    protected String getPrivateString(int columnIndex) throws Exception {
        return localDefaultResultSet.getString(columnIndex);
    }

    @Override
    protected int getPrivateInt(int columnIndex) throws Exception {
        return localDefaultResultSet.getInt(columnIndex);
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
        if (localDefaultResultSet != null) {
            try {
                try (java.sql.Statement resultSetStatement = localDefaultResultSet.getStatement()) {
                    localDefaultResultSet.close();
                    DatabaseCursorChecker.closeResultSet(this.getClass());
                } finally {
                    DatabaseCursorChecker.closeStatement(this.getClass());
                }
            } catch (SQLException ex) {
                exception = new FedException(String.format("%s: An error occurred while closing local result sets.%n%s",
                        this.getClass().getSimpleName(), ex.getMessage()), ex);
            }
        }

        rsMetdata = null;

        return exception;
    }

    @Override
    protected void resolveStatement() throws FedException {
        String tableName = selectStatement.getTableIdentifiers().get(0).getIdentifier();
        String queryStatement;
        if (hasFullyQualifiedAttributes()) {
            List<FullyQualifiedAttributeIdentifier> fqAttributes = selectStatement.getFQAttributeIdentifiers();
            String fqAttributesString = concatenateFQAttributes(fqAttributes);
            queryStatement = String.format("SELECT %s FROM %s", fqAttributesString, tableName);
        } else {
            queryStatement = String.format("SELECT * FROM %s", tableName);
        }
        localDefaultResultSet = getLocalResultSet(queryStatement, 1);
        rsMetdata = initialiseMetaData(localDefaultResultSet);
    }

    private boolean hasFullyQualifiedAttributes() {
        return selectStatement.getFQAttributeIdentifiers() != null && !selectStatement.getFQAttributeIdentifiers().isEmpty();
    }
}
