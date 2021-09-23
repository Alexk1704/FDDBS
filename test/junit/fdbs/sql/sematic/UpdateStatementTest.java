package junit.fdbs.sql.sematic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.semantic.SemanticValidator;
import fdbs.util.logger.Logger;
import java.sql.SQLException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class UpdateStatementTest {

    // <editor-fold defaultstate="collapsed" desc="Test set up and tear down" >
    private static FedConnection fedConnection;
    private static MetadataManager metadataManager;

    public UpdateStatementTest() {
    }

    @BeforeClass
    public static void setUpClass() throws FedException {

    }

    @AfterClass
    public static void tearDownClass() throws FedException {

    }

    @Before
    public void setUp() throws FedException, SQLException {
        Logger.getLogger();
        Logger.infoln("Start to test update statement semantic validator.");

        fedConnection = new FedConnection("VDBSA01", "VDBSA01");
        metadataManager = MetadataManager.getInstance(fedConnection, true);

        try {
            metadataManager.deleteTables();
        } catch (FedException ex) {
        }

        try {
            metadataManager.checkAndRefreshTables();
        } catch (FedException ex) {
        }

        String dropTableFLUGLINIE = "drop table FLUGLINIE";

        try {
            executeStatement(dropTableFLUGLINIE, 1);
        } catch (SQLException | FedException ex) {
        }

        try {
            executeStatement(dropTableFLUGLINIE, 2);
        } catch (SQLException | FedException ex) {
        }

        try {
            executeStatement(dropTableFLUGLINIE, 3);
        } catch (SQLException | FedException ex) {
        }

        String dropTableOrte = "drop table ORTE";

        try {
            executeStatement(dropTableOrte, 1);
        } catch (SQLException | FedException ex) {
        }

        try {
            executeStatement(dropTableOrte, 2);
        } catch (SQLException | FedException ex) {
        }

        try {
            executeStatement(dropTableOrte, 3);
        } catch (SQLException | FedException ex) {
        }
    }

    @After
    public void tearDown() throws FedException, SQLException {
        try {
            metadataManager.deleteTables();
        } catch (FedException ex) {
        }

        String dropTableFLUGLINIE = "drop table FLUGLINIE";

        try {
            executeStatement(dropTableFLUGLINIE, 1);
        } catch (SQLException | FedException ex) {
        }

        try {
            executeStatement(dropTableFLUGLINIE, 2);
        } catch (SQLException | FedException ex) {
        }

        try {
            executeStatement(dropTableFLUGLINIE, 3);
        } catch (SQLException | FedException ex) {
        }

        String dropTableOrte = "drop table ORTE";

        try {
            executeStatement(dropTableOrte, 1);
        } catch (SQLException | FedException ex) {
        }

        try {
            executeStatement(dropTableOrte, 2);
        } catch (SQLException | FedException ex) {
        }

        try {
            executeStatement(dropTableOrte, 3);
        } catch (SQLException | FedException ex) {
        }

        fedConnection.close();

        Logger.infoln("Finished to test update statement semantic validator.");
    }

    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Simple Update Tests" >
    @Test
    public void testTableDoesNotExists() throws ParseException, FedException, Exception {
        String updateFluglinie = "update FLUGLINIE set FLC = 'ABC'";

        try {
            parseValidateStatement(updateFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: A table with the name \"FLUGLINIE\" does not exists.", ex.getMessage());
        }
    }

    @Test
    public void testInvalidSetAttributeName() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String updateFluglinie = "update FLUGLINIE set Test = 'ABC'";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(updateFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: The attribute \"Test\" does not exists in the table \"FLUGLINIE\".", ex.getMessage());
        }
    }

    @Test
    public void testConvertSetAttributeTypeIntegerToVarChar() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String updateFluglinie = "update FLUGLINIE set FLC = 123";

        parseValidateHandleStatement(createFluglinie);
        parseValidateStatement(updateFluglinie);
    }

    @Test
    public void testConvertSetAttributeTypeVarCharToInteger() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String updateFluglinie = "update FLUGLINIE set FLC = '123'";

        parseValidateHandleStatement(createFluglinie);
        parseValidateStatement(updateFluglinie);
    }

    @Test
    public void testInvalidSetAttributeType() {
        String createFluglinie = "create table FLUGLINIE ( FLC integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String updateFluglinie = "update FLUGLINIE set FLC = 'ABC'";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(updateFluglinie);
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: The type from the attribute \"FLC\" is not equals the type of the setting value and the type of the setting value can not be converted into the attribute type.", ex.getMessage());
        }
    }

    @Test
    public void testWhereWithInvalidAttributeName() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "update FLUGLINIE set FLC = 'ABC' where Test > 0";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(deleteFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: Where clause - The attribute \"Test\" does not exists in the table \"FLUGLINIE\".", ex.getMessage());
        }
    }

    @Test
    public void testWhereWithInvalidAttributeConstantTypeComparisonVarChar() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "update FLUGLINIE set FLC = 'ABC' where FLC > 1";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(deleteFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: Where clause - The type from the attribute \"FLUGLINIE.FLC\" is not equals the type of the value.", ex.getMessage());
        }
    }

    @Test
    public void testWhereWithInvalidAttributeConstantTypeComparisonInteger() {
        String createFluglinie = "create table FLUGLINIE ( FLC integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String deleteFluglinie = "update FLUGLINIE set FLC = '1' where FLC > '1'";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(deleteFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: Where clause - The type from the attribute \"FLUGLINIE.FLC\" is not equals the type of the value.", ex.getMessage());
        }
    }

    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Primary Key Integrity Condition Tests" >
    // <editor-fold defaultstate="collapsed" desc="Primary Key Integrity Condition Tests without Update Where" >   
    @Test
    public void testValidUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('ABC', 1)"; // db1
        String validUpdate = "update FLUGLINIE set FLC = 'CVA'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testValidUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning2() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('ABC', 1)"; // db1
        String validUpdate = "update FLUGLINIE set FLC = 'ABC'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testValidUpdateNoneRowWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning2() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validUpdate = "update FLUGLINIE set FLC = 'ABC'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testInvalidUpdateOnMultipleRowsWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 1)"; // db1
        String validInsert2 = "insert into FLUGLINIE values('Test', 15)"; // db2
        String invalidUpdate = "update FLUGLINIE set FLC = 'WHATEVER'";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert1, 1);
            executeStatement(validInsert2, 2);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an update - This update violates the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testValidUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10,20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('ABC', 15)"; // db2
        String validUpdate = "update FLUGLINIE set FLC = 'CVA'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert, 2);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    @Test
    public void testValidUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning2() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10, 20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('ABC', 25)"; // db3
        String validUpdate = "update FLUGLINIE set FLC = 'ABC'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert, 3);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    @Test
    public void testInvalidUpdateOnMultipleRowsWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10, 15))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 30)"; // db3
        String invalidUpdate = "update FLUGLINIE set FLC = 'WHATEVER'";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert1, 2);
            executeStatement(validInsert2, 3);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an update - This update violates the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }

    @Test
    public void testValidUpdateNoneRowWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning2() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10, 15))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validUpdate = "update FLUGLINIE set FLC = 'ABC'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Primary Key Integrity Condition Tests with Update Where" >   
    @Test
    public void testInvalidWhereUpdateOnMultipleRowsWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 15)"; // db2
        String invalidUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 15";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert1, 2);
            executeStatement(validInsert2, 2);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an update - This update violates the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testInvalidWhereUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('WHATEVER', 1)"; // db1
        String invalidUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 15";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert1, 2);
            executeStatement(validInsert2, 1);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an update - This update violates the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testValidWhereUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1)"; // db1
        String validUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 15";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testValidWhereUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning2() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('WHATEVER', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1)"; // db1
        String validUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 15";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testValidWhereUpdateOnNoneRowWithAPrimaryKeyIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1)"; // db1
        String validUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 20";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testInvalidWhereUpdateOnMultipleRowsWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10,20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('Test', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('ABC', 15)"; // db2
        String validInsert3 = "insert into FLUGLINIE values('Test', 25)"; // db3
        String invalidUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 15";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert1, 2);
            executeStatement(validInsert2, 3);
            executeStatement(validInsert3, 3);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an update - This update violates the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }

    @Test
    public void testInvalidWhereUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10, 20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 25)"; // db3
        String validInsert2 = "insert into FLUGLINIE values('WHATEVER', 1)"; // db2
        String invalidUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 25";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert1, 3);
            executeStatement(validInsert2, 2);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an update - This update violates the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }

    @Test
    public void testValidWhereUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10, 20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1)"; // db1
        String validUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 15";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    @Test
    public void testValidWhereUpdateOnSingleRowWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning2() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10,20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('WHATEVER', 15)"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1)"; // db1
        String validInsert3 = "insert into FLUGLINIE values('Test2', 25)"; // db3
        String validUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 15";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);
        executeStatement(validInsert3, 3);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    @Test
    public void testValidWhereUpdateOnNoneRowWithAPrimaryKeyIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC)) horizontal (ID(10,20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 1)"; // db1
        String validInsert2 = "insert into FLUGLINIE values('Test', 15)"; // db2
        String validInsert3 = "insert into FLUGLINIE values('Test2', 25)"; // db3
        String validUpdate = "update FLUGLINIE set FLC = 'WHATEVER' Where ID = 20";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert1, 1);
        executeStatement(validInsert2, 2);
        executeStatement(validInsert3, 3);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    // </editor-fold>
    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Unique Integrity Condition Tests" > 
    // <editor-fold defaultstate="collapsed" desc="Unique Integrity Condition Tests without Update Where" > 
    @Test
    public void testValidUpdateOnSingleRowWithAUniqueIntegrityConditionByTwoHorizontalPartitioning() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('QWE', 1, 'ASD')"; // db1
        String validUpdate = "update FLUGLINIE set Name = 'CVA'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testValidUpdateOnSingleRowWithAUniqueIntegrityConditionByTwoHorizontalPartitioning2() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('QWE', 1, 'ABC')"; // db1
        String validUpdate = "update FLUGLINIE set Name = 'ABC'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testValidUpdateNoneRowWithAPUniqueIntegrityConditionByTwoHorizontalPartitioning2() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validUpdate = "update FLUGLINIE set Name = 'ABC'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testInvalidUpdateOnMultipleRowsWithAUniqueIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 1, 'QWE')"; // db1
        String validInsert2 = "insert into FLUGLINIE values('Test', 15, 'ASD')"; // db2
        String invalidUpdate = "update FLUGLINIE set Name = 'WHATEVER'";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert1, 1);
            executeStatement(validInsert2, 2);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: Unique constraint \"Name_U\" validation for an update - This update violates the unique constraint \"Name_U\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testValidUpdateOnSingleRowWithAUniqueConditionByThreeHorizontalPartitioning() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10,20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('ABC', 15, 'QWE')"; // db2
        String validUpdate = "update FLUGLINIE set Name = 'CVA'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert, 2);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    @Test
    public void testValidUpdateOnSingleRowWithAUniqueIntegrityConditionByThreeHorizontalPartitioning2() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10, 20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('QWE', 25, 'ABC')"; // db3
        String validUpdate = "update FLUGLINIE set Name = 'ABC'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert, 3);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    @Test
    public void testInvalidUpdateOnMultipleRowsWithAUniqueIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10, 15))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15, 'QWE')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 30, 'ASD')"; // db3
        String invalidUpdate = "update FLUGLINIE set Name = 'WHATEVER'";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert1, 2);
            executeStatement(validInsert2, 3);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: Unique constraint \"Name_U\" validation for an update - This update violates the unique constraint \"Name_U\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }

    @Test
    public void testValidUpdateNoneRowWithAUniqueIntegrityConditionByThreeHorizontalPartitioning2() throws FedException, SQLException, Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10, 15))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validUpdate = "update FLUGLINIE set Name = 'ABC'";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Unique Integrity Condition Tests with Update Where" >   
    @Test
    public void testInvalidWhereUpdateOnMultipleRowsWithAUniqueIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15, 'ASD')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 15, 'QWE')"; // db2
        String invalidUpdate = "update FLUGLINIE set Name = 'WHATEVER' Where ID = 15";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert1, 2);
            executeStatement(validInsert2, 2);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: Unique constraint \"Name_U\" validation for an update - This update violates the unique constraint \"Name_U\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testInvalidWhereUpdateOnSingleRowWithAUniqueIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15, 'QWE')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('WHATEVER', 1, 'ASD')"; // db1
        String invalidUpdate = "update FLUGLINIE set Name = 'ASD' Where ID = 15";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert1, 2);
            executeStatement(validInsert2, 1);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: Unique constraint \"Name_U\" validation for an update - This update violates the unique constraint \"Name_U\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testValidWhereUpdateOnSingleRowWithAUniqueIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15, 'QWE')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1, 'ASD')"; // db1
        String validUpdate = "update FLUGLINIE set Name = 'WHATEVER' Where ID = 15";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testValidWhereUpdateOnSingleRowWithAUniqueIntegrityConditionByTwoHorizontalPartitioning2() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('WHATEVER', 15, 'QWE')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1, 'ASD')"; // db1
        String validUpdate = "update FLUGLINIE set Name = 'QWE' Where ID = 15";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testValidWhereUpdateOnNoneRowWithAUniqueIntegrityConditionByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15, 'QWE')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1, 'ASD')"; // db1
        String validUpdate = "update FLUGLINIE set Name = 'WHATEVER' Where ID = 20";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
    }

    @Test
    public void testInvalidWhereUpdateOnMultipleRowsWithAUniqueIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10,20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('Test', 15, 'QWE')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('ABC', 15, 'ASD')"; // db2
        String validInsert3 = "insert into FLUGLINIE values('Test', 25, 'YXC')"; // db3
        String invalidUpdate = "update FLUGLINIE set Name = 'WHATEVER' Where ID = 15";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert1, 2);
            executeStatement(validInsert2, 3);
            executeStatement(validInsert3, 3);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: Unique constraint \"Name_U\" validation for an update - This update violates the unique constraint \"Name_U\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }

    @Test
    public void testInvalidWhereUpdateOnSingleRowWithAUniqueIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10, 20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 25, 'QWE')"; // db3
        String validInsert2 = "insert into FLUGLINIE values('WHATEVER', 1, 'ASD')"; // db2
        String invalidUpdate = "update FLUGLINIE set Name = 'ASD' Where ID = 25";

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert1, 3);
            executeStatement(validInsert2, 2);

            try {
                parseValidateHandleStatement(invalidUpdate);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("UpdateStatementSemanticValidator: Unique constraint \"Name_U\" validation for an update - This update violates the unique constraint \"Name_U\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }

    @Test
    public void testValidWhereUpdateOnSingleRowWithAUniqueIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10, 20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 15, 'QWE')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1, 'ASD')"; // db1
        String validUpdate = "update FLUGLINIE set Name = 'WHATEVER' Where ID = 15";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    @Test
    public void testValidWhereUpdateOnSingleRowWithAUpdateIntegrityConditionByThreeHorizontalPartitioning2() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10,20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ASD', 15, 'WHATEVER')"; // db2
        String validInsert2 = "insert into FLUGLINIE values('Test', 1, 'ASD')"; // db1
        String validInsert3 = "insert into FLUGLINIE values('Test2', 25, 'QWE')"; // db3
        String validUpdate = "update FLUGLINIE set Name = 'WHATEVER' Where ID = 15";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert1, 2);
        executeStatement(validInsert2, 1);
        executeStatement(validInsert3, 3);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    @Test
    public void testValidWhereUpdateOnNoneRowWithAUniqueIntegrityConditionByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), ID integer, Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_U unique(Name)) horizontal (ID(10,20))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert1 = "insert into FLUGLINIE values('ABC', 1)"; // db1
        String validInsert2 = "insert into FLUGLINIE values('Test', 15)"; // db2
        String validInsert3 = "insert into FLUGLINIE values('Test2', 25)"; // db3
        String validUpdate = "update FLUGLINIE set Name = 'WHATEVER' Where ID = 20";

        executeStatement(createTabeSmt, 1);
        executeStatement(createTabeSmt, 2);
        executeStatement(createTabeSmt, 3);
        executeStatement(validInsert1, 1);
        executeStatement(validInsert2, 2);
        executeStatement(validInsert3, 3);

        parseValidateHandleStatement(validUpdate);

        executeStatement(dropTable, 1);
        executeStatement(dropTable, 2);
        executeStatement(dropTable, 3);
    }

    // </editor-fold>
    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Foreign Key Integrity Condition Tests" >
    @Test
    public void testValidNullForeignKeyInsert() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) vertical((FLC,ID),(OrtName))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String validForeignKeyUpdate = "update FLUGLINIE set OrtName = null";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement(createReferenceTable, 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);

        executeStatement("insert into ORTE values('Fulda', 36037, 64008)", 1);
        executeStatement("insert into FLUGLINIE values('Flug1', 1)", 1);
        executeStatement("insert into FLUGLINIE values('Flug1', NULL)", 2);

        parseValidateHandleStatement(validForeignKeyUpdate);

        executeStatement(dropForeignKeyTable, 1);
        executeStatement(dropForeignKeyTable, 2);
        executeStatement(dropReferenceTable, 1);
    }

    @Test
    public void testAttributeIsNotInForeignKeyConstraint() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name))";
        String createReferenceTable = "create table ORTE (Name varchar(100), constraint ORTE_NAME primary key (Name)) horizontal (Name('A','M'))";
        String validNoneForeignKeyUpdate = "update FLUGLINIE set ID = 2";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());

        parseValidateHandleStatement(validNoneForeignKeyUpdate);
    }

    @Test
    public void testInvalidForeignKeySetVerticalDefault() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) vertical((FLC,ID),(OrtName))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String invalidForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement(createReferenceTable, 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);

        try {
            parseValidateHandleStatement(invalidForeignKeyUpdate);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
        } finally {
            executeStatement(dropForeignKeyTable, 1);
            executeStatement(dropForeignKeyTable, 2);
            executeStatement(dropReferenceTable, 1);
        }
    }

    @Test
    public void testInvalidForeignKeySetVerticalVertical() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) vertical((FLC), (ID),(OrtName))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name)) vertical((Name, PLZ),(Einwohner))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String invalidForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement("create table ORTE (Name varchar(100), PLZ integer, constraint ORTE_NAME primary key (Name))", 1);
        executeStatement("create table ORTE (Name varchar(100), Einwohner Integer, constraint ORTE_NAME primary key (Name))", 2);

        executeStatement("create table FLUGLINIE ( FLC varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 3);

        try {
            parseValidateHandleStatement(invalidForeignKeyUpdate);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
        } finally {
            executeStatement(dropForeignKeyTable, 1);
            executeStatement(dropForeignKeyTable, 2);
            executeStatement(dropForeignKeyTable, 3);
            executeStatement(dropReferenceTable, 1);
            executeStatement(dropReferenceTable, 2);
        }
    }

    @Test
    public void testInvalidForeignKeySetHorizontalDefault() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) horizontal (FLC('A','M'))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String invalidForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement(createReferenceTable, 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 3);

        try {
            parseValidateHandleStatement(invalidForeignKeyUpdate);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
        } finally {
            executeStatement(dropForeignKeyTable, 1);
            executeStatement(dropForeignKeyTable, 2);
            executeStatement(dropForeignKeyTable, 3);
            executeStatement(dropReferenceTable, 1);
        }
    }

    @Test
    public void testInvalidForeignKeySetHorizontalHorizontal() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) horizontal (FLC('A','M'))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name)) horizontal (Name('A','M'))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String invalidForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(3, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(2));
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(3));

        executeStatement("create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))", 1);
        executeStatement("create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))", 2);
        executeStatement("create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))", 3);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 3);

        try {
            parseValidateHandleStatement(invalidForeignKeyUpdate);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
        } finally {
            executeStatement(dropForeignKeyTable, 1);
            executeStatement(dropForeignKeyTable, 2);
            executeStatement(dropForeignKeyTable, 3);
            executeStatement(dropReferenceTable, 1);
            executeStatement(dropReferenceTable, 2);
            executeStatement(dropReferenceTable, 3);
        }
    }

    @Test
    public void testInvalidForeignKeySetHorizontalVertical() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) horizontal (FLC('A','M'))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name)) vertical((PLZ),(Einwohner,Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String invalidForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(2));

        executeStatement("create table ORTE (Name varchar(100), PLZ integer, constraint ORTE_NAME primary key (Name))", 1);
        executeStatement("create table ORTE (Name varchar(100), Einwohner Integer, constraint ORTE_NAME primary key (Name))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 3);

        try {
            parseValidateHandleStatement(invalidForeignKeyUpdate);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("UpdateStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
        } finally {
            executeStatement(dropForeignKeyTable, 1);
            executeStatement(dropForeignKeyTable, 2);
            executeStatement(dropForeignKeyTable, 3);
            executeStatement(dropReferenceTable, 1);
            executeStatement(dropReferenceTable, 2);
        }
    }

    @Test
    public void testValidForeignKeySetVerticalDefault() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) vertical((FLC,ID),(OrtName))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String validForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement(createReferenceTable, 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);

        executeStatement("insert into ORTE values('Fulda', 36037, 64008)", 1);
        executeStatement("insert into FLUGLINIE values('Flug1', 1)", 1);
        executeStatement("insert into FLUGLINIE values('Flug1', NULL)", 2);

        parseValidateHandleStatement(validForeignKeyUpdate);

        executeStatement(dropForeignKeyTable, 1);
        executeStatement(dropForeignKeyTable, 2);
        executeStatement(dropReferenceTable, 1);
    }

    @Test
    public void testValidForeignKeySetVerticalVertical() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) vertical((FLC),(ID),(OrtName))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name)) vertical((Name, PLZ),(Einwohner))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String validForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement("create table ORTE (Name varchar(100), PLZ integer, constraint ORTE_NAME primary key (Name))", 1);
        executeStatement("create table ORTE (Name varchar(100), Einwohner Integer, constraint ORTE_NAME primary key (Name))", 2);

        executeStatement("create table FLUGLINIE ( FLC varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 3);

        executeStatement("insert into ORTE values('Fulda', 36037)", 1);
        executeStatement("insert into ORTE values('Fulda', 64008)", 2);

        executeStatement("insert into FLUGLINIE values('Flug1')", 1);
        executeStatement("insert into FLUGLINIE values('Flug1', 2)", 2);
        executeStatement("insert into FLUGLINIE values('Flug1', NULL)", 3);

        parseValidateHandleStatement(validForeignKeyUpdate);

        executeStatement(dropForeignKeyTable, 1);
        executeStatement(dropForeignKeyTable, 2);
        executeStatement(dropForeignKeyTable, 3);
        executeStatement(dropReferenceTable, 1);
        executeStatement(dropReferenceTable, 2);
    }

    @Test
    public void testValidForeignKeySetHorizontalDefault() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) horizontal (FLC('A','M'))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String validForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement(createReferenceTable, 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 3);

        executeStatement("insert into ORTE values('Fulda', 36037, 64008)", 1);
        executeStatement("insert into FLUGLINIE values('ZForDB3', 1, NULL)", 3);

        parseValidateHandleStatement(validForeignKeyUpdate);

        executeStatement(dropForeignKeyTable, 1);
        executeStatement(dropForeignKeyTable, 2);
        executeStatement(dropForeignKeyTable, 3);
        executeStatement(dropReferenceTable, 1);
    }

    @Test
    public void testValidForeignKeySetHorizontalHorizontal() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) horizontal (FLC('A','M'))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name)) horizontal (Name('A','M'))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String validForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(3, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(2));
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(3));

        executeStatement("create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))", 1);
        executeStatement("create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))", 2);
        executeStatement("create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))", 3);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 3);

        executeStatement("insert into ORTE values('Fulda', 36037, 64008)", 1);
        executeStatement("insert into FLUGLINIE values('ZForDB3', 1, NULL)", 3);

        parseValidateHandleStatement(validForeignKeyUpdate);

        executeStatement(dropForeignKeyTable, 1);
        executeStatement(dropForeignKeyTable, 2);
        executeStatement(dropForeignKeyTable, 3);
        executeStatement(dropReferenceTable, 1);
        executeStatement(dropReferenceTable, 2);
        executeStatement(dropReferenceTable, 3);
    }

    @Test
    public void testValidForeignKeySetHorizontalVertical() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) horizontal (FLC('A','M'))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name)) vertical((PLZ),(Einwohner,Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String validForeignKeyUpdate = "update FLUGLINIE set OrtName = 'Fulda'";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(2));

        executeStatement("create table ORTE (Name varchar(100), PLZ integer, constraint ORTE_NAME primary key (Name))", 1);
        executeStatement("create table ORTE (Name varchar(100), Einwohner Integer, constraint ORTE_NAME primary key (Name))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 3);

        executeStatement("insert into ORTE values('Fulda', 36037)", 1);
        executeStatement("insert into ORTE values('Fulda', 64008)", 2);
        executeStatement("insert into FLUGLINIE values('ZForDB3', 1, NULL)", 3);

        parseValidateHandleStatement(validForeignKeyUpdate);

        executeStatement(dropForeignKeyTable, 1);
        executeStatement(dropForeignKeyTable, 2);
        executeStatement(dropForeignKeyTable, 3);
        executeStatement(dropReferenceTable, 1);
        executeStatement(dropReferenceTable, 2);
    }

    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Private Test Helper Functions" >
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

    private void executeStatement(String stmt, Integer db) throws FedException, SQLException {
        SQLExecuter sqlExecuter = new SQLExecuter(fedConnection);
        try (SQLExecuterTask task = new SQLExecuterTask()) {
            if (db == 0) {
                task.addSubTask(stmt, true, 1);
                task.addSubTask(stmt, true, 2);
                task.addSubTask(stmt, true, 3);
            } else if (db == 1) {
                task.addSubTask(stmt, true, 1);
            } else if (db == 2) {
                task.addSubTask(stmt, true, 2);
            } else if (db == 3) {
                task.addSubTask(stmt, true, 3);
            }
            
            sqlExecuter.executeTask(task);
        }
    }
    // </editor-fold>
}
