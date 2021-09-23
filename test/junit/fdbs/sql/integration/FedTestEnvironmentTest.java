/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.integration;

import fdbs.app.federated.FedTestEnvironment;
import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedPseudoDriver;
import fdbs.sql.meta.MetadataManager;
import fdbs.util.logger.Logger;
import java.io.FileNotFoundException;
import java.util.logging.Level;
import static junit.fdbs.sql.integration.IntegrationTest.fedCon;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Nicolai
 */
public class FedTestEnvironmentTest {
    
    
    @BeforeClass
    public static void setUpClass() throws FedException {
    }

    @AfterClass
    public static void tearDownClass() throws FedException {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
    }

    @After
    public void tearDown() {
        Logger.infoln("FedTestEnvironment tests finished...");
    }

    @Test
    public void testSystem() {
        /**
         * Logging
         *
         * -Doracle.jdbc.Trace=true
         * -Djava.util.logging.config.file=config.properties
         */
        final String usernameTest = "VDBSA03";
        final String passwordTest = "VDBSA03";

        final String usernameValidation = "VDBSA03";
        final String passwordValidation = "VDBSA03";

        FedConnection fedConnection;

        /*
         * Test schema
         */
        try {
            fedConnection = (new FedPseudoDriver()).getConnection(usernameValidation, passwordValidation);

            final FedTestEnvironment fedTestEvironment = new FedTestEnvironment(fedConnection);

            fedTestEvironment.run("Test/DRPTABS.SQL", false);
            fedTestEvironment.run("Test/CREPARTABS.SQL", false);
            fedTestEvironment.run("Test/INSERTAIRPORTS.SQL", false);
            fedTestEvironment.run("Test/INSERTAIRLINES.SQL", false);
            fedTestEvironment.run("Test/INSERTPASSENGERS.SQL", false);
            fedTestEvironment.run("Test/INSERTFLIGHTS.SQL", false);
            fedTestEvironment.run("Test/INSERTBOOKINGS.SQL", false);
//            fedTestEvironment.run("Test/PARSELCNTSTAR.SQL", true);
//            fedTestEvironment.run("Test/PARSELS1T.SQL", true);
//            fedTestEvironment.run("Test/PARSELS1OR.SQL", true);
//            fedTestEvironment.run("Test/PARSELSJOIN1.SQL", true);
//            fedTestEvironment.run("Test/PARSELS1TGP.SQL", true);
//            fedTestEvironment.run("Test/PARSELS1TWGP.SQL", true);
//            fedTestEvironment.run("Test/PARSELS1TGHAV.SQL", true);
            fedTestEvironment.run("Test/PARUPDS.SQL", true);
//            fedTestEvironment.run("Test/PARINSERTS.SQL", true);
            fedTestEvironment.run("Test/PARDELS.SQL", true);
//            fedTestEvironment.run("Test/PARSELCNTSTAR.SQL", true);
        } catch (final FedException fedException) {
            fedException.printStackTrace();

        }

        /*
                 * Validation schema
         */
        try {
            fedConnection = (new FedPseudoDriver()).getConnection(usernameValidation, passwordValidation);

            final FedTestEnvironment fedTestEvironment = new FedTestEnvironment(fedConnection);

            fedTestEvironment.run("Validation/DRPTABS.SQL", false);
            fedTestEvironment.run("Validation/CREPARTABS.SQL", false);
            fedTestEvironment.run("Validation/INSERTAIRPORTS.SQL", false);
            fedTestEvironment.run("Validation/INSERTAIRLINES.SQL", false);
            fedTestEvironment.run("Validation/INSERTPASSENGERS.SQL", false);
            fedTestEvironment.run("Validation/INSERTFLIGHTS.SQL", false);
            fedTestEvironment.run("Validation/INSERTBOOKINGS.SQL", false);
//            fedTestEvironment.run("Validation/PARSELCNTSTAR.SQL", true);

            fedTestEvironment.run("Validation/INSERT-PASSAGIER-10K-5.SQL", false);
            fedTestEvironment.run("Validation/INSERT-BUCHUNG-10K-5.SQL", false);
//            fedTestEvironment.run("Validation/PARSELCNTSTAR.SQL", true);

//            fedTestEvironment.run("Validation/PARSEL1TSmall.SQL", true);
//            fedTestEvironment.run("Validation/PARSEL1TLarge.SQL", true);
//            fedTestEvironment.run("Validation/PARSEL1TOR.SQL", true);
//            fedTestEvironment.run("Validation/PARSELJoinNormal.SQL", true);
//            fedTestEvironment.run("Validation/PARSEL1TGPSmall.SQL", true);
//            fedTestEvironment.run("Validation/PARSEL1TGPLarge.SQL", true);
//            fedTestEvironment.run("Validation/PARSEL1TWGPSmall.SQL", true);
//            fedTestEvironment.run("Validation/PARSEL1TWGPLarge.SQL", true);
//            fedTestEvironment.run("Validation/PARSELJoinWGP.SQL", true);
//            fedTestEvironment.run("Validation/PARSEL1TGHAVSmall.SQL", true);
//            fedTestEvironment.run("Validation/PARSEL1TGHAVLarge.SQL", true);
//            fedTestEvironment.run("Validation/PARSELCNTSTAR.SQL", true);

        } catch (final FedException fedException) {
            fedException.printStackTrace();
        }
    }
}
