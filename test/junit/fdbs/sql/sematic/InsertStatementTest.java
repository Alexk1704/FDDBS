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

public class InsertStatementTest {

    // <editor-fold defaultstate="collapsed" desc="Test set up and tear down" >
    private static FedConnection fedConnection;
    private static MetadataManager metadataManager;

    public InsertStatementTest() {
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
        Logger.infoln("Start to test insert statement semantic validator.");

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
    public void tearDown() {
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

        Logger.infoln("Finished to test insert statement semantic validator.");
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Simple Insert Tests" >
    @Test
    public void testTableDoesNotExists() throws ParseException, FedException, Exception {
        String insertFluglinie = "insert into FLUGLINIE values('ABC')";

        try {
            parseValidateStatement(insertFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: A table with the name \"FLUGLINIE\" does not exists.", ex.getMessage());
        }
    }

    @Test
    public void testInsufficientNumberOfValues() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), Test integer, Test2 integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String insertFluglinie = "insert into FLUGLINIE values('ABC')";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(insertFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: The number of values is insufficient.", ex.getMessage());
        }
    }

    @Test
    public void testToManyNumberOfValues() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), Test integer, Test2 integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String insertFluglinie = "insert into FLUGLINIE values('ABC', 123, 123, 123)";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(insertFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: The number of values is too many.", ex.getMessage());
        }
    }

    @Test
    public void testInsertWithInvalidValueType() {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), Test integer, Test2 integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String insertFluglinie = "insert into FLUGLINIE values('ABC', 'ABC', 123)";

        try {
            parseValidateHandleStatement(createFluglinie);
            parseValidateStatement(insertFluglinie);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: The type from the attribute \"Test\" is not equals the type of the setting value and the type of the setting value can not be converted into the attribute type.", ex.getMessage());
        }
    }

    @Test
    public void testConvertSetAttributeTypeIntegerToVarChar() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), Test varchar(100), Test2 integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String insertFluglinie = "insert into FLUGLINIE values('ABC', 123, 123)";

        parseValidateHandleStatement(createFluglinie);
        parseValidateStatement(insertFluglinie);
    }

    @Test
    public void testConvertSetAttributeTypeVarCharToInteger() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(2), Test integer, Test2 integer, constraint FLUGLINIE_FLC primary key (FLC))";
        String insertFluglinie = "insert into FLUGLINIE values('ABC', '1', 123)";

        parseValidateHandleStatement(createFluglinie);
        parseValidateStatement(insertFluglinie);
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Primary Key Integrity Condition Tests" >
    @Test
    public void testPrimaryKeyIntegrityConditionsWithInvalidVarCharInsertByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC)) horizontal (Name('M'))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100),  Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('A', 'A')"; // db1 
        String invalidInsert = "insert into FLUGLINIE values('A', 'N')"; // db2

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert, 1);

            try {
                parseValidateHandleStatement(invalidInsert);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("InsertStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an insert - The value \"A\" is not unique to the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testPrimaryKeyIntegrityConditionsWithInvalidIntegerInsertByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC integer,  Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC)) horizontal (Name('M'))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC integer,  Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values(1, 'A')"; // db1 
        String invalidInsert = "insert into FLUGLINIE values(1, 'N')"; // db2

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert, 1);

            try {
                parseValidateHandleStatement(invalidInsert);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("InsertStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an insert - The value \"1\" is not unique to the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testPrimaryKeyIntegrityConditionsWithInvalidVarCharInsertByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100),  Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC)) horizontal (Name('M','P'))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100),  Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('A', 'A')"; // db1 
        String validInsert2 = "insert into FLUGLINIE values('B', 'N')"; // db2
        String invalidInsert = "insert into FLUGLINIE values('B', 'Z')"; // db3

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert, 1);
            executeStatement(validInsert2, 2);

            try {
                parseValidateHandleStatement(invalidInsert);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("InsertStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an insert - The value \"B\" is not unique to the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }

    @Test
    public void testPrimaryKeyIntegrityConditionsWithInvalidIntegerInsertByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC integer,  Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC)) horizontal (Name('M','Z'))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_FLC").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC integer,  Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values(1, 'A')"; // db1 
        String validInsert2 = "insert into FLUGLINIE values(2, 'N')"; // db2
        String invalidInsert = "insert into FLUGLINIE values(2, 'Z')"; // db3

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert, 1);
            executeStatement(validInsert2, 2);

            try {
                parseValidateHandleStatement(invalidInsert);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("InsertStatementSemanticValidator: PrimaryKey constraint \"FLUGLINIE_FLC\" validation for an insert - The value \"2\" is not unique to the primarykey constraint \"FLUGLINIE_FLC\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Unique Integrity Condition Tests" >
    @Test
    public void testUniqueIntegrityConditionsWithInvalidVarCharInsertByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_Name unique(Name)) horizontal (FLC('M'))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_Name").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_Name unique(Name))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('A', 'A')"; //db1
        String invalidInsert = "insert into FLUGLINIE values('N', 'A')"; // db2

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert, 1);

            try {
                parseValidateHandleStatement(invalidInsert);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("InsertStatementSemanticValidator: Unique constraint \"FLUGLINIE_Name\" validation for an insert - The value \"A\" is not unique to the unique constraint \"FLUGLINIE_Name\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testUniqueIntegrityConditionsWithInvalidIntegerInsertByTwoHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), Name integer, constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_Name unique(Name)) horizontal (FLC('M'))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_Name").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_Name unique(Name))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('A', 1)"; //db1
        String invalidInsert = "insert into FLUGLINIE values('N', 1)"; // db2

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(validInsert, 1);

            try {
                parseValidateHandleStatement(invalidInsert);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("InsertStatementSemanticValidator: Unique constraint \"FLUGLINIE_Name\" validation for an insert - The value \"1\" is not unique to the unique constraint \"FLUGLINIE_Name\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
        }
    }

    @Test
    public void testUniqueIntegrityConditionsWithInvalidVarCharInsertByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_Name unique(Name)) horizontal (FLC('M', 'Z'))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_Name").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_Name unique(Name))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('A', 'A')"; //db1
        String vaildInsert2 = "insert into FLUGLINIE values('N', 'B')"; // db2
        String invalidInsert = "insert into FLUGLINIE values('Z', 'B')"; // db3

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert, 1);
            executeStatement(vaildInsert2, 2);

            try {
                parseValidateHandleStatement(invalidInsert);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("InsertStatementSemanticValidator: Unique constraint \"FLUGLINIE_Name\" validation for an insert - The value \"B\" is not unique to the unique constraint \"FLUGLINIE_Name\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }

    @Test
    public void testUniqueIntegrityConditionsWithInvalidIntegerInsertByThreeHorizontalPartitioning() throws Exception {
        String createFluglinie = "create table FLUGLINIE ( FLC varchar(100), Name integer, constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_Name unique(Name)) horizontal (FLC('M','Z'))";
        parseValidateHandleStatement(createFluglinie);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "FLUGLINIE_Name").getDb());

        String createTabeSmt = "create table FLUGLINIE ( FLC varchar(100), Name varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint FLUGLINIE_Name unique(Name))";
        String dropTable = "drop table FLUGLINIE";
        String validInsert = "insert into FLUGLINIE values('A', 1)"; //db1
        String validInsert2 = "insert into FLUGLINIE values('N', 2)"; // db2
        String invalidInsert = "insert into FLUGLINIE values('Z', 2)"; // db2

        try {
            executeStatement(createTabeSmt, 1);
            executeStatement(createTabeSmt, 2);
            executeStatement(createTabeSmt, 3);
            executeStatement(validInsert, 1);
            executeStatement(validInsert2, 1);

            try {
                parseValidateHandleStatement(invalidInsert);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertEquals("InsertStatementSemanticValidator: Unique constraint \"FLUGLINIE_Name\" validation for an insert - The value \"2\" is not unique to the unique constraint \"FLUGLINIE_Name\".", ex.getMessage());
            }
        } finally {
            executeStatement(dropTable, 1);
            executeStatement(dropTable, 2);
            executeStatement(dropTable, 3);
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Foreign Key Integrity Condition Tests" >
    @Test
    public void testValidNullForeignKeyInsert() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) vertical((FLC,ID),(OrtName))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String validForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, null)";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement(createReferenceTable, 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);

        executeStatement("insert into ORTE values('Fulda', 36037, 64008)", 1);

        parseValidateHandleStatement(validForeignKeyInsert);

        executeStatement(dropForeignKeyTable, 1);
        executeStatement(dropForeignKeyTable, 2);
        executeStatement(dropReferenceTable, 1);
    }

    @Test
    public void testInvalidForeignKeySetVerticalDefault() throws Exception {
        String createForeignKeyTable = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Name_FK FOREIGN KEY(OrtName) REFERENCES ORTE(Name)) vertical((FLC,ID),(OrtName))";
        String createReferenceTable = "create table ORTE (Name varchar(100), PLZ integer, Einwohner Integer, constraint ORTE_NAME primary key (Name))";
        String dropForeignKeyTable = "drop table FLUGLINIE";
        String dropReferenceTable = "drop table ORTE";
        String invalidForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement(createReferenceTable, 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);

        try {
            parseValidateHandleStatement(invalidForeignKeyInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
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
        String invalidForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

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
            parseValidateHandleStatement(invalidForeignKeyInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
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
        String invalidForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

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
            parseValidateHandleStatement(invalidForeignKeyInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
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
        String invalidForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

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
            parseValidateHandleStatement(invalidForeignKeyInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
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
        String invalidForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

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
            parseValidateHandleStatement(invalidForeignKeyInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Foreign Key constraint \"Name_FK\" validation - The foreign key constraint value \"Fulda\" is unknown.", ex.getMessage());
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
        String validForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

        parseValidateHandleStatement(createReferenceTable);
        parseValidateHandleStatement(createForeignKeyTable);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Name_FK").getDb());
        Assert.assertEquals(1, metadataManager.getTable("ORTE").getDatabasesForColumn("Name").size());
        Assert.assertTrue(metadataManager.getTable("ORTE").getDatabasesForColumn("Name").contains(1));

        executeStatement(createReferenceTable, 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), ID integer, constraint FLUGLINIE_FLC primary key (FLC))", 1);
        executeStatement("create table FLUGLINIE ( FLC varchar(100), OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC))", 2);

        executeStatement("insert into ORTE values('Fulda', 36037, 64008)", 1);

        parseValidateHandleStatement(validForeignKeyInsert);

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
        String validForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

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

        parseValidateHandleStatement(validForeignKeyInsert);

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
        String validForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

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

        parseValidateHandleStatement(validForeignKeyInsert);

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
        String validForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

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

        parseValidateHandleStatement(validForeignKeyInsert);

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
        String validForeignKeyInsert = "insert into FLUGLINIE values('FlugXY', 1, 'Fulda')";

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

        parseValidateHandleStatement(validForeignKeyInsert);

        executeStatement(dropForeignKeyTable, 1);
        executeStatement(dropForeignKeyTable, 2);
        executeStatement(dropForeignKeyTable, 3);
        executeStatement(dropReferenceTable, 1);
        executeStatement(dropReferenceTable, 2);
    }

    // </editor-fold>
    
    // <editor-fold defaultstate="collapsed" desc="Check Condition Tests" >
    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckVerticalAndDifferentTypes() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, OrtName varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID = OrtName)) vertical ((FLC, ID),(OrtName))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 36037, 'Fulda')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - Can not validate a expression which have two different attribute/value types.", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckEqualsIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID = ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 36037, 123)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"36037\" is not equals then \"123\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckGreaterIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID > ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 123, 1234)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"123\" is not greater then \"1234\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckLowerIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID < ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 1234, 123)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"1234\" is not lower then \"123\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckGreaterequalIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID >= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 123, 1234)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"123\" is not greaterequal then \"1234\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckLowerequalIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID <= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 1234, 123)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"1234\" is not lowerequal then \"123\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckNotqualIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID != ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 123, 123)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"123\" is not notequals then \"123\".", ex.getMessage());
        }
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckEqualsIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID = ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 123, 123)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckGreaterIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID > ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 1234, 123)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckLowerIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID < ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 123, 1234)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckGreaterequalIntegerVertical1() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID >= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 1234, 1234)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());
        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckGreaterequalIntegerVertical2() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID >= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 12345, 1234)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());
        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckLowerequalIntegerVertical1() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID <= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 1234, 1234)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckLowerequalIntegerVertical2() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID <= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 123, 1234)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckNotqualIntegerVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID integer, ID2 integer, constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID != ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 123, 1234)";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckEqualsVarCharVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID = ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'ABC', 'AB')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"ABC\" is not equals then \"AB\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckGreaterVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID > ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'A', 'B')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"A\" is not greater then \"B\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckLowerVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID < ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'B', 'A')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"B\" is not lower then \"A\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckGreaterequalVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID >= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'A', 'B')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"A\" is not greaterequal then \"B\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckLowerequalVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID <= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'B', 'A')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"B\" is not lowerequal then \"A\".", ex.getMessage());
        }
    }

    @Test
    public void testInvalidAttributeAttributeBinaryExpressionCheckNotqualVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID != ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'A', 'A')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        try {
            parseValidateHandleStatement(invalidInsert);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("InsertStatementSemanticValidator: Check constraint \"Check_AA\" validation - The binary expression value \"A\" is not notequals then \"A\".", ex.getMessage());
        }
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckEqualsVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID = ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'ABC', 'ABC')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckGreaterVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID > ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'ABCD', 'ABC')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckLowerVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID < ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'A', 'B')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckGreaterequalVarChaVertical1() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID >= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'ABC', 'ABC')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());
        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckGreaterequalVarChaVertical2() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID >= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'a', 'A')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());
        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckLowerequalVarChaVertical1() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID <= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', '1', '123')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckLowerequalVarChaVertical2() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID <= ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', '1b', '1b')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
    }

    @Test
    public void testAttributeAttributeBinaryExpressionCheckNotqualVarChaVertical() throws Exception {
        String createFLUGLINIE = "create table FLUGLINIE ( FLC varchar(100), ID varchar(100), ID2 varchar(100), constraint FLUGLINIE_FLC primary key (FLC), constraint Check_AA check (ID != ID2)) vertical ((FLC, ID),(ID2))";
        String invalidInsert = "insert into FLUGLINIE values('Test', 'Abc1', 'ABC1')";

        parseValidateHandleStatement(createFLUGLINIE);
        Assert.assertEquals(0, metadataManager.getConstraint("FLUGLINIE", 0, "Check_AA").getDb());

        parseValidateHandleStatement(invalidInsert);
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
