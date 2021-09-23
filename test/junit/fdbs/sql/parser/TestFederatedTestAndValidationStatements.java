package junit.fdbs.sql.parser;

import fdbs.app.federated.FedTestEnvironment;
import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.util.logger.Logger;
import java.util.Scanner;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFederatedTestAndValidationStatements {

    public TestFederatedTestAndValidationStatements() {
    }

    @BeforeClass
    public static void setUpClass() {
        Logger.setLogLevel(Logger.LogLevel.DEBUG);
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test parsing test and validation statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing test and validation statement.");
    }

    @Test
    public void testFederatedTestAndValidationStatements() throws ParseException {
        test("Test/DRPTABS.SQL");
        test("Test/CREPARTABS.SQL");
        test("Test/INSERTAIRPORTS.SQL");
        test("Test/INSERTAIRLINES.SQL");
        test("Test/INSERTPASSENGERS.SQL");
        test("Test/INSERTFLIGHTS.SQL");
        test("Test/INSERTBOOKINGS.SQL");
        test("Test/PARSELCNTSTAR.SQL");
        test("Test/PARSELS1T.SQL");
        test("Test/PARSELS1OR.SQL");
        test("Test/PARSELSJOIN1.SQL");
        test("Test/PARSELS1TGP.SQL");
        test("Test/PARSELS1TWGP.SQL");
        test("Test/PARSELS1TGHAV.SQL");
        test("Test/PARUPDS.SQL");
        test("Test/PARINSERTS.SQL");
        test("Test/PARDELS.SQL");
        test("Test/PARSELCNTSTAR.SQL");

        test("Validation/DRPTABS.SQL");
        test("Validation/CREPARTABS.SQL");
        test("Validation/INSERTAIRPORTS.SQL");
        test("Validation/INSERTAIRLINES.SQL");
        test("Validation/INSERTPASSENGERS.SQL");
        test("Validation/INSERTFLIGHTS.SQL");
        test("Validation/INSERTBOOKINGS.SQL");
        test("Validation/PARSELCNTSTAR.SQL");
        test("Validation/INSERT-PASSAGIER-10K-5.SQL");
        test("Validation/INSERT-BUCHUNG-10K-5.SQL");
        test("Validation/PARSELCNTSTAR.SQL");
        test("Validation/PARSEL1TSmall.SQL");
        test("Validation/PARSEL1TLarge.SQL");
        test("Validation/PARSEL1TOR.SQL");
        test("Validation/PARSELJoinNormal.SQL");
        test("Validation/PARSEL1TGPSmall.SQL");
        test("Validation/PARSEL1TGPLarge.SQL");
        test("Validation/PARSEL1TWGPSmall.SQL");
        test("Validation/PARSEL1TWGPLarge.SQL");
        test("Validation/PARSELJoinWGP.SQL");
        test("Validation/PARSEL1TGHAVSmall.SQL");
        test("Validation/PARSEL1TGHAVLarge.SQL");
        test("Validation/PARSELCNTSTAR.SQL");
    }

    public void test(String filename) throws ParseException {
        Logger.infoln(String.format("Test Statements from File: %s \n", filename));

        final Scanner scanner = new Scanner(FedTestEnvironment.class.getResourceAsStream(filename)).useDelimiter(";");
        while (scanner.hasNext()) {
            String statement = scanner.next().trim();

            if (statement.toUpperCase().startsWith("CREATE")
                    || statement.toUpperCase().startsWith("DROP")
                    || statement.toUpperCase().startsWith("DELETE")
                    || statement.toUpperCase().startsWith("INSERT")
                    || statement.toUpperCase().startsWith("UPDATE")
                    || statement.toUpperCase().startsWith("SELECT")) {

                Logger.infoln(String.format("Test Statement: %s", statement));

                SqlParser parser = new SqlParser(statement);
                AST ast = parser.parseStatement();
                Assert.assertTrue(ast != null);
            }
        }
    }
}
