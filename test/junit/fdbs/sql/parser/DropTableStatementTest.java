package junit.fdbs.sql.parser;

import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.statement.DropTableStatement;
import fdbs.util.logger.Logger;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DropTableStatementTest {

    public DropTableStatementTest() {
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
        Logger.infoln("Start to test parsing drop table statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing drop table statement.");
    }

    @Test
    public void testDropTableStatement() throws ParseException {
        SqlParser parser = new SqlParser("drop table BUCHUNG");
        AST ast = parser.parseStatement();
        DropTableStatement stmt = (DropTableStatement) ast.getRoot();

        Assert.assertEquals("BUCHUNG", stmt.getTableIdentifier().getIdentifier());
        Assert.assertTrue(!stmt.hasCascadeConstraintsExtension());
    }

    @Test
    public void testDropTableStatementWithCascade() throws ParseException {
        SqlParser parser = new SqlParser("drop table BUCHUNG cascade constraints");
        AST ast = parser.parseStatement();
        DropTableStatement stmt = (DropTableStatement) ast.getRoot();

        Assert.assertEquals("BUCHUNG", stmt.getTableIdentifier().getIdentifier());
        Assert.assertTrue(stmt.hasCascadeConstraintsExtension());
    }
}
