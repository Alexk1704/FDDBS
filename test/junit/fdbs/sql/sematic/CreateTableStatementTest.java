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

public class CreateTableStatementTest {

    private static FedConnection fedConnection;
    private static MetadataManager metadataManager;

    public CreateTableStatementTest() {
    }

    @BeforeClass
    public static void setUpClass() throws FedException {
        fedConnection = new FedConnection("VDBSA08", "VDBSA08");
        metadataManager = MetadataManager.getInstance(fedConnection);
    }

    @AfterClass
    public static void tearDownClass() throws FedException {
        fedConnection.close();
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test create table statement semantic validator.");

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

        Logger.infoln("Finished to test create table statement semantic validator.");
    }

    @Test
    public void testTwoSameTableNames() throws ParseException, FedException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        parseValidateHandleStatement(createFluglinie);

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: A table with the name \"FLUGLINIE\" does already exists.", ex.getMessage());
        }
    }

    @Test
    public void testTwoSameAttributes() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: The attriubte name \"FLC\" defined multiple times.", ex.getMessage());
        }
    }

    @Test
    public void testTwoSameConstraintNames() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_FLC check (LAND is not null))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: The constraint with the name \"FLUGLINIE_FLC\" does exists multiple times in this create table statement.", ex.getMessage());
        }
    }

    @Test
    public void testSameConstraintNameExistsAlready() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateHandleStatement(createFluglinie2);
            Assert.fail();
        } catch (Exception ex) {
            Logger.error("FEHLER", ex);
            Assert.assertEquals("CreateTableStatementSemanticValidator: A constraint with the name \"FLUGLINIE_FLC\" does already exists.", ex.getMessage());
        }
    }

    @Test
    public void testInvalidVarCharDefaultValue() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2) default(1), constraint FLUGLINIE_FLC primary key (FLC))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: The attriubte \"FLC\" default value has a invalid type.", ex.getMessage());
        }
    }

    @Test
    public void testInvalidIntegerDefaultValue() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC integer default('1'), constraint FLUGLINIE_FLC primary key (FLC))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: The attriubte \"FLC\" default value has a invalid type.", ex.getMessage());
        }
    }

    @Test
    public void testVerticalPartitioningUnknowAttribute() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC integer default(1), Test integer, Test2 integer, constraint FLUGLINIE_FLC primary key (FLC)) VERTICAL((FLC, Test),(Test2, Test3))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Vertical Partitioning - The attribute \"Test3\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testIncompleteVerticalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC integer default(1), Test integer, Test2 integer, Test3 integer, constraint FLUGLINIE_FLC primary key (FLC)) VERTICAL((FLC),(Test2))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Vertical Partitioning - The following attributes have no vertical partitioning assignment: Test;Test3.", ex.getMessage());
        }
    }

    @Test
    public void testAttributeIsInMultipleVerticalTables() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC integer default(1), Test integer, constraint FLUGLINIE_FLC primary key (FLC)) VERTICAL((FLC),(FLC))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Vertical Partitioning - The attribute \"FLC\" is already defined for database 1.", ex.getMessage());
        }
    }

    @Test
    public void testHorizontalPartitioningAttributeInvalid() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC)) HORIZONTAL (Name('K'))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Horizontal Partitioning - The attribute \"Name\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testHorizontalPartitioningInvalidFirstBoundaryTypes() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC)) HORIZONTAL (FLC(3))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Horizontal Partitioning - The first boundary value has a invalid type.", ex.getMessage());
        }
    }

    @Test
    public void testHorizontalPartitioningInvalidSecondBoundaryTypes() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC)) HORIZONTAL (FLC('D',1))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Horizontal Partitioning - The second boundary value has a invalid type.", ex.getMessage());
        }
    }

    @Test
    public void testHorizontalPartitioningInvalidVarCharBoundaryRanges() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC)) HORIZONTAL (FLC('D','A'))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Horizontal Partitioning - The first and the second are together invalid. The second boundary must be greater than the first boundary.", ex.getMessage());
        }
    }

    @Test
    public void testHorizontalPartitioningInvalidIntegerBoundaryRanges() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC)) HORIZONTAL (Test(55,1))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Horizontal Partitioning - The first and the second are together invalid. The second boundary must be greater than the first boundary.", ex.getMessage());
        }
    }

    @Test
    public void testPrimaryKeyAttributeIsInvalid() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (Test3))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Primary Key Constraint \"FLUGLINIE_FLC\" - The attribute name \"Test3\" was not definied.", ex.getMessage());
        }
    }

    @Test
    public void testUniqueAttributeIsInvalid() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint unique_test unique(test3))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Unique Constraint \"unique_test\" - The attribute name \"test3\" was not definied.", ex.getMessage());
        }
    }

    @Test
    public void testCheckNotNullAttributeIsInvalid() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint check_test check(test3 is not null))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Check Constraint \"check_test\" - The attribute \"test3\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testCheckBetweenAttributeIsInvalid() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint check_test check(test3 between 1 and 2))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Check Constraint \"check_test\" - The attribute \"test3\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testCheckACComparisonAttributeIsInvalid() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint check_test check(test3 > 2))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Check Constraint \"check_test\" - The attribute \"test3\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testCheckAAComparisonAttributeIsInvalid1() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint check_test check(test3 > Test))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Check Constraint \"check_test\" - The attribute \"test3\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testCheckAAComparisonAttributeIsInvalid2() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint check_test check(Test > test3))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Check Constraint \"check_test\" - The attribute \"test3\" is unknown.", ex.getMessage());
        }
    }

    @Test
    public void testForeignKeyAttributeIsInvalid() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint fk_test foreign key(test3) references FLUGLINIE2(FLC))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Foreign Key Constraint \"fk_test\" - The attribute name \"test3\" was not definied.", ex.getMessage());
        }
    }

    @Test
    public void testForeignKeyRefTableIsInvalid() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint fk_test foreign key(Test) references FLUGLINIE2(FLC))";

        try {
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Foreign Key Constraint \"fk_test\" - The referenced table \"FLUGLINIE2\" does not exists.", ex.getMessage());
        }
    }

    @Test
    public void testForeignKeyRefTablePrimayKeyAttributeDoesNotExists() {
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(2), constraint FLUGLINIE_FLC2 primary key (FLC))";
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint fk_test foreign key(Test) references FLUGLINIE2(Test))";

        try {
            parseValidateHandleStatement(createFluglinie2);
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Foreign Key Constraint \"fk_test\" - The referenced attribute name \"Test\" does not exists in the referenced table \"FLUGLINIE2\".", ex.getMessage());
        }
    }

    @Test
    public void testForeignKeyRefRefAttributeInvalidType() {
        String createFluglinie2 = "create table FLUGLINIE2 ( FLC varchar(100), constraint FLUGLINIE_FLC2 primary key (FLC))";
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100) default('1'), Test integer, constraint FLUGLINIE_FLC primary key (FLC), constraint fk_test foreign key(Test) references FLUGLINIE2(FLC))";

        try {
            parseValidateHandleStatement(createFluglinie2);
            parseValidateHandleStatement(createFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("CreateTableStatementSemanticValidator: Foreign Key Constraint \"fk_test\" - The attribute type is incompatible with the referenced attribute type.", ex.getMessage());
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
        ast.getRoot().accept(new SemanticValidator(fedConnection));
        return ast;
    }

    private void handleStatement(AST ast) throws FedException {
        metadataManager.handleQuery(ast);
    }
}
