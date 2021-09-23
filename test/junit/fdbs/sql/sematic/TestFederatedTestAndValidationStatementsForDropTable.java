package junit.fdbs.sql.sematic;

import junit.fdbs.sql.parser.*;
import fdbs.app.federated.FedTestEnvironment;
import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.semantic.SemanticValidator;
import fdbs.util.logger.Logger;
import java.util.Scanner;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFederatedTestAndValidationStatementsForDropTable {

    private static FedConnection fedConnection;
    private static MetadataManager metadataManager;

    public TestFederatedTestAndValidationStatementsForDropTable() {
    }

    @BeforeClass
    public static void setUpClass() throws FedException {
        fedConnection = new FedConnection("VDBSA01", "VDBSA01");
        metadataManager = MetadataManager.getInstance(fedConnection);
    }

    @AfterClass
    public static void tearDownClass() throws FedException {
        fedConnection.close();
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test parsing test and validation statement.");

        try {
            metadataManager.deleteTables();
        } catch (FedException ex) {
        }

        try {
            metadataManager.checkAndRefreshTables();
        } catch (FedException ex) {
        }
    }

    @After
    public void tearDown() {
        try {
            metadataManager.deleteTables();
        } catch (FedException ex) {
        }

        Logger.infoln("Finished to test parsing test and validation statement.");
    }

    @Test
    public void testFederatedTestAndValidationStatements() throws ParseException, Exception {

        test("Test/CREPARTABS.SQL");
        test("Test/DRPTABS.SQL"); //change order from drop table and create table

        test("Validation/CREPARTABS.SQL");
        test("Validation/DRPTABS.SQL"); //change order from drop table and create table
    }

    public void test(String filename) throws ParseException, Exception {
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

                parseValidateHandleStatement(statement);
                Assert.assertTrue(true);
            }
        }
    }

    private void parseValidateHandleStatement(String statement) throws ParseException, Exception {
        handleStatement(validateAST(parseStatement(statement)));
    }

    private AST parseStatement(String statement) throws ParseException {
        SqlParser parserCreateDB = new SqlParser(statement);
        return parserCreateDB.parseStatement();
    }

    private AST validateAST(AST ast) throws FedException, Exception {
        Assert.assertTrue(ast != null);
        ast.getRoot().accept(new SemanticValidator(fedConnection));
        return ast;
    }

    private void handleStatement(AST ast) throws FedException {
        metadataManager.handleQuery(ast);
    }
}
