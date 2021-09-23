package fdbs.app.federated;

import fdbs.sql.*;

public class App {

    public static void main(final String[] args) {

        /**
         * Logging
         *
         * -Doracle.jdbc.Trace=true
         * -Djava.util.logging.config.file=config.properties
         */
        final String usernameTest = "PROJA04";
        final String passwordTest = "XXXXXX";

        final String usernameValidation = "PROJA04";
        final String passwordValidation = "XXXXXX";

        FedConnection fedConnection;

        /*
         * Test schema
         */
        try {
            fedConnection = (new FedPseudoDriver()).getConnection(usernameValidation, passwordValidation);

            final FedTestEnvironment fedTestEvironment = new FedTestEnvironment(fedConnection);

            fedTestEvironment.run("fdbs/app/federated/test/DRPTABS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/test/CREPARTABS.SQL", false);

            fedTestEvironment.run("fdbs/app/federated/test/INSERTAIRPORTS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/test/INSERTAIRLINES.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/test/INSERTPASSENGERS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/test/INSERTFLIGHTS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/test/INSERTBOOKINGS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/test/PARSELCNTSTAR.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/test/PARSELS1T.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/test/PARSELS1OR.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/test/PARSELSJOIN1.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/test/PARSELS1TGP.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/test/PARSELS1TWGP.SQL", true);   //OPTIONAL
            fedTestEvironment.run("fdbs/app/federated/test/PARSELS1TGHAV.SQL", true);  //OPTIONAL
            fedTestEvironment.run("fdbs/app/federated/test/PARUPDS.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/test/PARINSERTS.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/test/PARDELS.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/test/PARSELCNTSTAR.SQL", true);

        } catch (final FedException fedException) {
            fedException.printStackTrace();

        }

        /*
		 * Validation schema
         */
        try {
            fedConnection = (new FedPseudoDriver()).getConnection(usernameValidation, passwordValidation);

            final FedTestEnvironment fedTestEvironment = new FedTestEnvironment(fedConnection);

            fedTestEvironment.run("fdbs/app/federated/validation/DRPTABS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/CREPARTABS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/INSERTAIRPORTS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/INSERTAIRLINES.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/INSERTPASSENGERS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/INSERTFLIGHTS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/INSERTBOOKINGS.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/PARSELCNTSTAR.SQL", true);

            fedTestEvironment.run("fdbs/app/federated/validation/INSERT-PASSAGIER-10K-5.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/INSERT-BUCHUNG-10K-5.SQL", false);
            fedTestEvironment.run("fdbs/app/federated/validation/PARSELCNTSTAR.SQL", true);

            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TSmall.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TLarge.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TOR.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/validation/PARSELJoinNormal.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TGPSmall.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TGPLarge.SQL", true);
            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TWGPSmall.SQL", true);  // OPTIONAL
            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TWGPLarge.SQL", true);  // OPTIONAL
            fedTestEvironment.run("fdbs/app/federated/validation/PARSELJoinWGP.SQL", true);  // OPTIONAL     
            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TGHAVSmall.SQL", true);  // OPTIONAL
            fedTestEvironment.run("fdbs/app/federated/validation/PARSEL1TGHAVLarge.SQL", true);  // OPTIONAL
            fedTestEvironment.run("fdbs/app/federated/validation/PARSELCNTSTAR.SQL", true);

        } catch (final FedException fedException) {
            fedException.printStackTrace();
        }
    }
}
