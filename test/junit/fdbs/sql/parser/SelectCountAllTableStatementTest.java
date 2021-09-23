/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.parser;

import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.statement.select.SelectCountAllTableStatement;
import fdbs.util.logger.Logger;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Patrick
 */
public class SelectCountAllTableStatementTest {

    public SelectCountAllTableStatementTest() {
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
        Logger.infoln("Start to test parsing insert statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing select count all table statement.");
    }

    @Test
    public void testSelectCountAllTableStatement() throws ParseException {
        SqlParser parser = new SqlParser("select count(*) from FLUGLINIE");
        AST ast = parser.parseStatement();
        SelectCountAllTableStatement stmt = (SelectCountAllTableStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
    }
}
