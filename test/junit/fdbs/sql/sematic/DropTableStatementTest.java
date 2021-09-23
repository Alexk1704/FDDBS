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

public class DropTableStatementTest {

    private static FedConnection fedConnection;
    private static MetadataManager metadataManager;

    public DropTableStatementTest() {
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
        Logger.infoln("Start to test drop table statement semantic validator.");

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

        Logger.infoln("Finished to test drop table statement semantic validator.");
    }

    @Test
    public void testCreateTableStatementSemanticValidator() throws ParseException, FedException, Exception {
        createTableBUCHUNG();
        createTablePASSAGIER();

        try {
            dropTableFlUG();
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("DropTableStatementSemanticValidator: A table with the name \"FLUG\" does not exists.", ex.getMessage());
        }

        try {
            dropTableFLUGLINIE();
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertEquals("DropTableStatementSemanticValidator: A table with the name \"FLUGLINIE\" does not exists.", ex.getMessage());
        }

        dropTableBUCHUNG();
        dropTablePASSAGIER();
    }

    private void createTableBUCHUNG() throws ParseException, Exception {
        String stmt = "create table BUCHUNG (FHC varchar(3), constraint FLUGHAFEN_PS primary key (FHC))";
        parseValidateHandleStatement(stmt);
    }

    private void createTablePASSAGIER() throws ParseException, Exception {
        String stmt = "create table PASSAGIER (FHC varchar(3), constraint FLUGHAFEN_PS2 primary key (FHC))";
        parseValidateHandleStatement(stmt);
    }

    private void dropTableBUCHUNG() throws ParseException, Exception {
        String stmt = "drop table BUCHUNG cascade constraints";
        parseValidateHandleStatement(stmt);
    }

    private void dropTablePASSAGIER() throws ParseException, Exception {
        String stmt = "drop table PASSAGIER cascade constraints";
        parseValidateHandleStatement(stmt);
    }

    private void dropTableFlUG() throws ParseException, Exception {
        String stmt = "drop table FLUG cascade constraints";
        parseValidateHandleStatement(stmt);
    }

    private void dropTableFLUGLINIE() throws ParseException, Exception {
        String stmt = "drop table FLUGLINIE cascade constraints";
        parseValidateHandleStatement(stmt);
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
