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

public class SelectGroupStatementTest {

    private static FedConnection fedConnection;
    private static MetadataManager metadataManager;

    public SelectGroupStatementTest() {
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
        Logger.infoln("Start to test select group statement semantic validator.");

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

        Logger.infoln("Finished to test select group statement semantic validator.");
    }

    @Test
    public void testTable1DoesNotExists() throws ParseException, FedException, Exception {
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 group by FLUGLINIE2.FLC";

        try {
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: A table with the name \"FLUGLINIE\" does not exists.", ex.getMessage());
        }
    }

    @Test
    public void testTable2DoesNotExists() throws ParseException, FedException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: A table with the name \"FLUGLINIE2\" does not exists.", ex.getMessage());
        }
    }

    @Test
    public void testFQAttributeWithInvalidTable() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE3.Test, Count(*) from FLUGLINIE, FLUGLINIE2 group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.Test\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testFQAttributeWithInvalidAttribute() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.Test, Count(*) from FLUGLINIE, FLUGLINIE2 group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: The attribute \"Test\" does not exists in the table \"FLUGLINIE2\".", ex.getMessage());
        }
    }

    @Test
    public void testFunctionWithInvalidTable() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, max(FLUGLINIE3.Test) from FLUGLINIE, FLUGLINIE2 group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: MAX Function - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.Test\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testFunctionWithInvalidAttribute() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, sum(FLUGLINIE2.Test) from FLUGLINIE, FLUGLINIE2 group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: SUM Function - The attribute \"Test\" does not exists in the table \"FLUGLINIE2\".", ex.getMessage());
        }
    }

    @Test
    public void testGroubByWithInvalidTable() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, max(FLUGLINIE.FLC) from FLUGLINIE, FLUGLINIE2 group by FLUGLINIE3.Test";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Group By - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.Test\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testGroubByWithInvalidAttribute() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, sum(FLUGLINIE.FLC) from FLUGLINIE, FLUGLINIE2 group by FLUGLINIE2.Test";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Group By - The attribute \"Test\" does not exists in the table \"FLUGLINIE2\".", ex.getMessage());
        }
    }

    @Test
    public void testNonJoinOrNonJoinWithInvalidTable1() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE ((FLUGLINIE3.FLC > 'AB') OR (FLUGLINIE2.FLC > 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.FLC\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testNonJoinOrNonJoinWithInvalidTable2() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE ((FLUGLINIE2.FLC > 'AB') OR (FLUGLINIE3.FLC > 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.FLC\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testNonJoinOrNonJoinWithInvalidAttribute1() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE ((FLUGLINIE.Test > 'AB') OR (FLUGLINIE2.FLC > 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The attribute \"Test\" does not exists in the table \"FLUGLINIE\".", ex.getMessage());
        }
    }

    @Test
    public void testNonJoinOrNonJoinWithInvalidAttribute2() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE ((FLUGLINIE.FLC > 'AB') OR (FLUGLINIE2.Test > 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The attribute \"Test\" does not exists in the table \"FLUGLINIE2\".", ex.getMessage());
        }
    }

    @Test
    public void testNonJoinOrNonJoinWithInvalidTypesVarChar() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE ((FLUGLINIE.FLC > 1) OR (FLUGLINIE2.FLC > 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The type from the attribute \"FLUGLINIE.FLC\" is not equals the type of the value.", ex.getMessage());
        }
    }

    @Test
    public void testJoinWithInvalidTypesInteger() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC integer, constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE ((FLUGLINIE.FLC > 'AB') OR (FLUGLINIE2.FLC > 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The type from the attribute \"FLUGLINIE2.FLC\" is not equals the type of the value.", ex.getMessage());
        }
    }

    @Test
    public void testJoinWithInvalidTable1() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE (FLUGLINIE3.FLC = FLUGLINIE2.FLC) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.FLC\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testJoinWithInvalidTable2() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE (FLUGLINIE.FLC = FLUGLINIE3.FLC) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.FLC\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testJoinWithInvalidAttribute1() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE (FLUGLINIE.Test <= FLUGLINIE2.FLC) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The attribute \"Test\" does not exists in the table \"FLUGLINIE\".", ex.getMessage());
        }
    }

    @Test
    public void testJoinWithInvalidAttribute2() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE (FLUGLINIE.FLC <= FLUGLINIE2.Test) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The attribute \"Test\" does not exists in the table \"FLUGLINIE2\".", ex.getMessage());
        }
    }

    @Test
    public void testJoinWithNonJoinNonJoinWithInvalidJoin() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE (FLUGLINIE3.FLC = FLUGLINIE2.FLC) AND ((FLUGLINIE1.FLC > 'AB') OR (FLUGLINIE2.FLC <= 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.FLC\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testJoinWithNonJoinNonJoinWithInvalidNonJoin1() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE (FLUGLINIE.FLC = FLUGLINIE2.FLC) AND ((FLUGLINIE3.FLC > 'AB') OR (FLUGLINIE2.FLC <= 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.FLC\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testJoinWithNonJoinNonJoinWithInvalidNonJoin2() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE2_FLC primary key (FLC))";
        String selectFluglinie = "select FLUGLINIE2.FLC, Count(*) from FLUGLINIE, FLUGLINIE2 WHERE (FLUGLINIE.FLC = FLUGLINIE2.FLC) AND ((FLUGLINIE.FLC > 'AB') OR (FLUGLINIE3.FLC <= 'AB')) group by FLUGLINIE2.FLC";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            parseValidateStatement(selectFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("SelectStatementSemanticValidator: Where clause - The table name \"FLUGLINIE3\" from the full qualified attribute \"FLUGLINIE3.FLC\" is unknown.", ex.getMessage());
        }
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
