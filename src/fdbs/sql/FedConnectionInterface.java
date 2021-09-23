package fdbs.sql;

import java.sql.SQLException;

public interface FedConnectionInterface {

    void setAutoCommit(boolean autoCommit) throws FedException;

    boolean getAutoCommit() throws FedException, SQLException;

    void commit() throws FedException;

    void rollback() throws FedException;

    void close() throws FedException;

    FedStatement getStatement() throws FedException;
}
