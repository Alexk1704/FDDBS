package junit.fdbs.sql;

import fdbs.sql.FedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import fdbs.sql.PropertyManager;
import fdbs.util.logger.Logger;

/**
 * PropertyManagerTest-class is Unit-test which tests the PropertyManager-class
 */
public class PropertyManagerTest {

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test :");
    }

    @After
    public void tearDown() {
        Logger.infoln("End of FedConnection class test.");
    }

    /**
     * Loads each property from the properties-file and check if its not null
     */
    @Test
    public void testAllProperties() throws FedException {
        assertNotNull("Should not be NULL", PropertyManager.getInstance().getConnectionStringByID(1));
        assertNotNull("Should not be NULL", PropertyManager.getInstance().getConnectionStringByID(2));
        assertNotNull("Should not be NULL", PropertyManager.getInstance().getConnectionStringByID(3));
        assertNotNull("Should not be NULL", PropertyManager.getInstance().getLogLevel());
    }
}
