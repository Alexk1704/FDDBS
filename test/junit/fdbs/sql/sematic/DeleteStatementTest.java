package junit.fdbs.sql.sematic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.semantic.SemanticValidator;
import fdbs.util.logger.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class DeleteStatementTest {

    private static FedConnection fedConnection;
    private static MetadataManager metadataManager;

    public DeleteStatementTest() {
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
        Logger.infoln("Start to test delete statement semantic validator.");

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

        Logger.infoln("Finished to test delete statement semantic validator.");
    }

    @Test
    public void testTableDoesNotExists() throws ParseException, FedException, Exception {
        String deleteFluglinie = "delete from FLUGLINIE";

        try {
            parseValidateStatement(deleteFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("DeleteStatementSemanticValidator: A table with the name \"FLUGLINIE\" does not exists.", ex.getMessage());
        }
    }

    @Test
    public void testWhereWithInvalidAttributeName() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "delete from FLUGLINIE where Test > 0";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(deleteFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("DeleteStatementSemanticValidator: Where clause - The attribute \"Test\" does not exists in the table \"FLUGLINIE\".", ex.getMessage());
        }
    }

    @Test
    public void testWhereWithInvalidAttributeConstantTypeComparisonVarChar() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "delete from FLUGLINIE where FLC > 1";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(deleteFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("DeleteStatementSemanticValidator: Where clause - The type from the attribute \"FLUGLINIE.FLC\" is not equals the type of the value.", ex.getMessage());
        }
    }

    @Test
    public void testWhereWithInvalidAttributeConstantTypeComparisonInteger() {
        String createFluglinie = "create table FLUGLINIE ( FLC integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "delete from FLUGLINIE where FLC > '1'";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(deleteFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("DeleteStatementSemanticValidator: Where clause - The type from the attribute \"FLUGLINIE.FLC\" is not equals the type of the value.", ex.getMessage());
        }
    }

    @Test
    public void testValidFullDelete() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "delete from FLUGLINIE";

        parseValidateHandleStatement(createFluglinie);
        parseValidateStatement(deleteFluglinie);
    }

    @Test
    public void testValidDeleteWithVarCharWhere() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(119), constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "delete from FLUGLINIE where FLC > 'Test123!'";

        parseValidateHandleStatement(createFluglinie);
        parseValidateStatement(deleteFluglinie);
    }

    @Test
    public void testValidDeleteWithIntegerWhere() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "delete from FLUGLINIE where FLC > 1";

        parseValidateHandleStatement(createFluglinie);
        parseValidateStatement(deleteFluglinie);
    }

    private void parseValidateHandleStatement(String statement) throws ParseException, Exception {
        handleStatement(validateAST(parseStatement(statement)));
    }

    private void parseValidateStatement(String statement) throws ParseException, Exception {
        validateAST(parseStatement(statement));
    }

    private AST parseStatement(String statement) throws ParseException {
        SqlParser parserCreateDB = new SqlParser(statement);
        return parserCreateDB.parseStatement();
    }

    private AST validateAST(AST ast) throws FedException, Exception {
        ast.getRoot().accept(new SemanticValidator(fedConnection));
        return ast;
    }

    private void handleStatement(AST ast) throws FedException {
        metadataManager.handleQuery(ast);
    }
}
