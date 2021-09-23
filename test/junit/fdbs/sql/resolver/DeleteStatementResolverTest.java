/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.resolver;

import fdbs.sql.FedException;
import fdbs.sql.FedPseudoDriver;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.statement.CreateTableStatement;
import fdbs.sql.parser.ast.statement.DeleteStatement;
import fdbs.sql.parser.ast.statement.InsertStatement;

import fdbs.sql.resolver.CreateStatementResolver;
import fdbs.sql.resolver.DeleteStatementResolver;
import fdbs.sql.resolver.InsertStatementResolver;

import fdbs.util.logger.Logger;
import java.sql.ResultSet;

import org.junit.AfterClass;
import org.junit.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author patrick
 */
public class DeleteStatementResolverTest {

    String[] statements;
    fdbs.sql.FedConnection fed;
    private TestHelper helper;

    public DeleteStatementResolverTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test general fed result set.");
        try {
            if (fed == null) {
                fed = (new FedPseudoDriver()).getConnection("VDBSA04", "VDBSA04");
                System.out.println("NEW FED");
            }

        } catch (FedException ex) {
            Logger.log(ex.getMessage());
        }
        helper = new TestHelper(fed);

    }

    /**
     * Test of resolveStatement method, of class InsertStatementResolver.
     */
    private void executeStatements(String[] statements) throws Exception {
        SQLExecuter executer = new SQLExecuter(fed);
        for (String stm : statements) {
            SqlParser parser = new SqlParser(stm);
            AST tree = parser.parseStatement();
            if (tree.getRoot() instanceof DeleteStatement) {
                DeleteStatementResolver r = new DeleteStatementResolver(fed);
                executer.executeTask(r.resolveStatement(tree));
            } else if (tree.getRoot() instanceof InsertStatement) {
                InsertStatementResolver r = new InsertStatementResolver(fed);
                executer.executeTask(r.resolveStatement(tree));
            } else if (tree.getRoot() instanceof CreateTableStatement) {
                CreateStatementResolver r = new CreateStatementResolver(fed);
                executer.executeTask(r.resolveStatement(tree));
            }
            System.out.println("Next statement...");
        }
    }

    @Test
    public void testResolveStatementVerticalWithThreeDBs() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) vertical((NAME),(STADT),(LAND, FHC)) ",
            "INSERT INTO FLUGHAFEN VALUES ('Auckland International', 'Auckland','NZ','AUI')",
            "DELETE FROM FLUGHAFEN"
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 0);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
        rs = task.getDb3Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
    }
    
    @Test
    public void testResolveStatementVerticalWithThreeDBsWhere() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) vertical((NAME),(STADT, FHC), (LAND)) ",
            "INSERT INTO FLUGHAFEN VALUES ('Auckland International', 'Auckland', 'AUI', 'NZ')",
            "INSERT INTO FLUGHAFEN VALUES ('Berlin Brandenburg', 'Berlin', 'BER', 'DE')",            
            "DELETE FROM FLUGHAFEN WHERE FHC = 'AUI'"
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("BER",rs.getString("FHC"));
        Assert.assertEquals(false,rs.next());
    }    

    @Test
    public void testResolveStatementVerticalWithTwoDBs() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) vertical((NAME, LAND),(STADT, FHC)) ",
            "INSERT INTO FLUGHAFEN VALUES ('Auckland International', 'NZ','Auckland','AUI')",
            "DELETE FROM FLUGHAFEN"
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 2);        
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
    }

    @Test
    public void testResolveStatementVerticalWithTwoDBsWhere() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) vertical((NAME, LAND),(STADT, FHC)) ",
            "INSERT INTO FLUGHAFEN VALUES ('Auckland International', 'NZ','Auckland','AUI')",
            "INSERT INTO FLUGHAFEN VALUES ('Berlin Brandenburg', 'DE','Berlin','BER')",            
            "DELETE FROM FLUGHAFEN WHERE FHC = 'AUI'"
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("BER",rs.getString("FHC"));
        Assert.assertEquals(false,rs.next());
    }    
    
    @Test
    public void testResolveStatementHorizontalWithThreeDBs() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal (NAME('L','R')) ",
            "INSERT INTO FLUGHAFEN VALUES ('AKL', 'NZ','Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('MU', 'DE','Munich','Munich International')",
            "INSERT INTO FLUGHAFEN VALUES ('SA', 'DE','Saarbrücken','Saarbrücken International')",
            "DELETE FROM FLUGHAFEN"
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 2);        
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 3);        
        rs = task.getDb3Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());        
    }    
    
    @Test
    public void testResolveStatementHorizontalWithThreeDBsWithWhere() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal (NAME('L','R')) ",
            "INSERT INTO FLUGHAFEN VALUES ('AKL', 'NZ','Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('MU', 'DE','Munich','Munich International')",
            "INSERT INTO FLUGHAFEN VALUES ('SA', 'DE','Saarbrücken','Saarbrücken International')",
            "DELETE FROM FLUGHAFEN WHERE FHC = 'MU'"
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        Assert.assertEquals(true, rs.next());
        Assert.assertEquals("AKL",rs.getString("FHC"));
        Assert.assertEquals(false, rs.next());     
        
        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 2);        
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
        
        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 3);        
        rs = task.getDb3Statements().get(0).getQueryResultSet();
        Assert.assertEquals(true, rs.next());
        Assert.assertEquals("SA",rs.getString("FHC"));
        Assert.assertEquals(false, rs.next());             
    }       
    
    @Test
    public void testResolveStatementHorizontalWithTwoDBs() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal (NAME('L')) ",
            "INSERT INTO FLUGHAFEN VALUES ('AKL', 'NZ','Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('MU', 'DE','Munich','Munich International')",
            "INSERT INTO FLUGHAFEN VALUES ('SA', 'DE','Saarbrücken','Saarbrücken International')",
            "DELETE FROM FLUGHAFEN"
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 2);        
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
    }    
    
    @Test
    public void testResolveStatementHorizontalWithTwoDBsWithWhere() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal (NAME('L')) ",
            "INSERT INTO FLUGHAFEN VALUES ('AKL', 'NZ','Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('MU', 'DE','Munich','Munich International')",
            "INSERT INTO FLUGHAFEN VALUES ('SA', 'DE','Saarbrücken','Saarbrücken International')",
            "DELETE FROM FLUGHAFEN WHERE FHC = 'AKL'"
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        Assert.assertEquals(false, rs.next());
        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 2);        
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("MU", rs.getString("FHC"));
        Assert.assertEquals(rs.next(),true);
        Assert.assertEquals("SA", rs.getString("FHC"));
        Assert.assertEquals(rs.next(),false);
    }         
}
