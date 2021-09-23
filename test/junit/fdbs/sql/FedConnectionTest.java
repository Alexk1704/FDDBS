package junit.fdbs.sql;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.util.logger.Logger;
import java.sql.Connection;
import java.util.HashMap;

/**
 * FedConnectionTest-class is Unit-test which tests the FedConnection-class
 */
public class FedConnectionTest {

    @BeforeClass
    public static void setUpClass() {

    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test FedConnection class:");
    }

    @After
    public void tearDown() {
        Logger.infoln("End of FedConnection class test.");
    }

    /**
     * Create a new FedConnection and check if the connections are not null.Also
     * Switches between connections to check that the selectedConnection works
     * flawless
     */
    @Test
    public void testFedConnection() throws FedException {

        FedConnection fedConnection = new FedConnection("VDBSB01", "VDBSB01");
        HashMap<Integer, Connection> connections = fedConnection.getConnections();
        Assert.assertNotNull("Connection to Database 1 should not be null", connections.get(1));
        Assert.assertNotNull("Connection to Database 2 should not be null", connections.get(2));
        Assert.assertNotNull("Connection to Database 3 should not be null", connections.get(3));
    }
}
