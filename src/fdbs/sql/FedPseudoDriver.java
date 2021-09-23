package fdbs.sql;

import fdbs.util.logger.Logger;

/**
 * FedPseudoDriver-class for calling the FedConnection after changing the user
 * specific data
 */
public class FedPseudoDriver implements FedPseudoDriverInterface {

    /**
     * The Constructor for a federated sql pseudo driver.
     * @throws fdbs.sql.FedException
     */
    public FedPseudoDriver() throws FedException {
        Logger.getLogger();
    }

    /**
     * @param username changes the user which will be used in all connections
     * @param password changes the password which will be used in all
     * connections
     *
     * @return returns a new instance of the FedConnection with the specific
     * user data
     *
     * @throws FedException
     */
    @Override
    public FedConnection getConnection(final String username, final String password) throws FedException {
        FedConnection fedConnection = new FedConnection(username, password);
        return fedConnection;
    }
}
