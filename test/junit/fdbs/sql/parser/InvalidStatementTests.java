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

public class InvalidStatementTests {

    public InvalidStatementTests() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test invalid statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test invalid statement.");
    }

    @Test
    public void testInvalidStatements() throws ParseException {

        final Scanner scanner = new Scanner(getClass().getResourceAsStream("InvalidStatements.sql")).useDelimiter(";");
        while (scanner.hasNext()) {
            String statement = scanner.next().trim();

            if (statement.toUpperCase().startsWith("CREATE")
                    || statement.toUpperCase().startsWith("DROP")
                    || statement.toUpperCase().startsWith("DELETE")
                    || statement.toUpperCase().startsWith("INSERT")
                    || statement.toUpperCase().startsWith("UPDATE")
                    || statement.toUpperCase().startsWith("SELECT")) {

                Logger.infoln(String.format("Test invalid Statement: %s", statement));

                try {
                    SqlParser parser = new SqlParser(statement);
                    AST ast = parser.parseStatement();
                    Assert.assertTrue(ast == null);
                } catch (ParseException ex) {
                    Logger.error("Test invalid statement error message:", ex);
                    Assert.assertTrue(true);
                }
            }
        }
    }
}
