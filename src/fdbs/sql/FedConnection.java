package fdbs.sql;

import fdbs.sql.meta.MetadataManager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import fdbs.util.logger.Logger;
import java.util.ArrayList;
import java.util.List;

/**
 * FedConnection-class for building the connections to all databases which are
 * need for the VDBS.
 */
public class FedConnection implements FedConnectionInterface, AutoCloseable {

    private String username;
    private String password;
    private boolean allConnectionAutoCommit = true; // default value
    private boolean isClosed = false;
    private HashMap<Integer, Connection> connections = new HashMap();
    private List<FedStatement> statements = new ArrayList<>();
    private MetadataManager metadataManager;

    /**
     * constructor builds all needed connections and safes the connections in a
     * Map
     *
     * @param username changes the user which will be used in all connections
     * @param password changes the password which will be used in all
     * @throws fdbs.sql.FedException
     */
    public FedConnection(String username, String password) throws FedException {

        this.username = username;
        this.password = password;

        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());

            PropertyManager propertyManager = PropertyManager.getInstance();

            int dbId = 1;
            while (true) {
                String connectionString = propertyManager.getConnectionStringByID(dbId);
                if (connectionString == null) {
                    break;
                }

                Connection connection = DriverManager.getConnection(formatConnectionString(connectionString));
                Logger.infoln(String.format("%s: Connected to \"%s\".", this.getClass().getSimpleName(), connection.getMetaData().getURL()));
                connections.put(dbId, connection);
                Logger.infoln(String.format("%s: Added connection %s as %d.", this.getClass().getSimpleName(), connection.getMetaData().getURL(), dbId));
                dbId++;
            }

            metadataManager = MetadataManager.getInstance(this, true);
        } catch (SQLException | FedException ex) {
            throw new FedException(String.format("%s: An error occurred while setting up database connections.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }
    }

    /**
     * @return returns a HashMap with all needed connections
     */
    public HashMap<Integer, Connection> getConnections() throws FedException {
        checkIsClosed();
        return connections;
    }

    /**
     * @param autoCommit set the autoCommit to true of false
     *
     * @throws FedException
     */
    @Override
    public void setAutoCommit(final boolean autoCommit) throws FedException {
        Logger.infoln(String.format("%s: Set auto commit of the federated connection to %s.", this.getClass().getSimpleName(), autoCommit));

        checkIsClosed();

        List<SQLException> occuredExceptions = new ArrayList<>();
        connections.keySet().forEach((key) -> {
            try {
                Connection connection = connections.get(key);
                connection.setAutoCommit(autoCommit);
            } catch (SQLException ex) {
                occuredExceptions.add(ex);
            }
        });

        if (occuredExceptions.size() > 0) {
            FedException fedException = new FedException(String.format("An error occurred while set auto commit.%n%s", this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));

            occuredExceptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            throw fedException;
        }

        Logger.infoln(String.format("%s: Did set auto commit of the federated connection to %s.", this.getClass().getSimpleName(), autoCommit));
    }

    /**
     * @return returns if the autoCommit is enabled or not
     *
     * @throws FedException
     * @throws SQLException
     */
    @Override
    public boolean getAutoCommit() throws FedException, SQLException {
        checkIsClosed();
        return allConnectionAutoCommit;
    }

    /**
     * CommitMessage for the selected connection
     *
     * @throws FedException
     */
    @Override
    public void commit() throws FedException {
        Logger.infoln(String.format("%s: Commit federated connection.", this.getClass().getSimpleName()));
        checkIsClosed();

        List<SQLException> occuredExceptions = new ArrayList<>();
        connections.keySet().forEach((key) -> {
            try {
                Connection connection = connections.get(key);
                connection.commit();
            } catch (SQLException ex) {
                occuredExceptions.add(ex);
            }
        });

        List<FedException> clearStatementExeptions = clearStatements();

        if (occuredExceptions.size() > 0) {
            FedException fedException = new FedException(String.format("%s: An error occurred while committing federated connection.%n%s", this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));

            occuredExceptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            clearStatementExeptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            throw fedException;
        }

        Logger.infoln(String.format("%s: Committed federated connection.", this.getClass().getSimpleName()));
    }

    /**
     * Rollback-method for the selected connection
     *
     * @throws FedException
     */
    @Override
    public void rollback() throws FedException {
        Logger.infoln(String.format("%s: Start rollback federated connection.", this.getClass().getSimpleName()));

        checkIsClosed();

        List<SQLException> occuredExceptions = new ArrayList<>();
        connections.keySet().forEach((key) -> {
            try {
                Connection connection = connections.get(key);
                connection.rollback();
            } catch (SQLException ex) {
                occuredExceptions.add(ex);
            }
        });

        List<FedException> clearStatementExeptions = clearStatements();

        FedException refreshMetadataException = null;
        try {
            metadataManager.checkAndRefreshTables();
        } catch (FedException ex) {
            refreshMetadataException = ex;
        }

        if (occuredExceptions.size() > 0 || clearStatementExeptions.size() > 0 || refreshMetadataException != null) {
            FedException fedException = new FedException(String.format("%s: An error occurred while rollback federated connection.%n%s", this.getClass().getSimpleName(), occuredExceptions.get(0).getMessage()));

            occuredExceptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            clearStatementExeptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            if (refreshMetadataException != null) {
                fedException.addSuppressed(refreshMetadataException);
            }

            throw fedException;
        }

        Logger.infoln(String.format("%s: Finished rollback federated connection.", this.getClass().getSimpleName()));
    }

    /**
     * Close-method for closing the selected connection
     *
     * @throws FedException
     */
    @Override
    public void close() throws FedException {
        Logger.infoln(String.format("%s: Closing federated connection.", this.getClass().getSimpleName()));

        checkIsClosed();

        isClosed = true;
        List<FedException> clearStatementExeptions = clearStatements();
        List<SQLException> clearConnectionExceptions = clearConnections();

        if (clearStatementExeptions.size() > 0 || clearConnectionExceptions.size() > 0) {
            FedException fedException;
            if (clearStatementExeptions.size() > 0) {
                fedException = new FedException(String.format("%s: An error occurred while closing federated connection.%n%s", this.getClass().getSimpleName(), clearStatementExeptions.get(0).getMessage()));
            } else {
                fedException = new FedException(String.format("%s: An error occurred while closing federated connection.%n%s", this.getClass().getSimpleName(), clearConnectionExceptions.get(0).getMessage()));
            }

            clearStatementExeptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            clearConnectionExceptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            throw fedException;
        }

        Logger.infoln(String.format("%s: Closed federated connection.", this.getClass().getSimpleName()));
    }

    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Gets a new statement.
     *
     * @return Returns a new statement.
     * @throws fdbs.sql.FedException
     */
    @Override
    public FedStatement getStatement() throws FedException {
        checkIsClosed();

        FedStatement statement = new FedStatement(this);
        statements.add(statement);
        return statement;
    }

    private void checkIsClosed() throws FedException {
        if (isClosed) {
            throw new FedException(String.format("%s: The federated connection is closed.", this.getClass().getSimpleName()));
        }
    }

    private List<SQLException> clearConnections() {
        List<SQLException> occuredExceptions = new ArrayList<>();
        connections.keySet().forEach((key) -> {
            try {
                Connection connection = connections.get(key);
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException ex) {
                occuredExceptions.add(ex);
            }
        });

        return occuredExceptions;
    }

    private List<FedException> clearStatements() {
        List<FedException> occuredExceptions = new ArrayList<>();
        statements.forEach((statement) -> {
            try {
                if (!statement.isClosed()) {
                    statement.close();
                }
            } catch (FedException ex) {
                occuredExceptions.add(ex);
            }
        });

        return occuredExceptions;
    }

    /**
     * @param connectionString Connection-String which should be formatted from
     * the properties-file
     *
     * @return returns the formatted database-String
     */
    private String formatConnectionString(String connectionString) {
        String userdata = String.format("%s/%s", username, password);
        return connectionString.replaceAll("%username%/%password%", userdata);
    }
}
