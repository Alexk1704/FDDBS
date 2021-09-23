/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedPseudoDriver;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.statement.CreateTableStatement;
import fdbs.sql.parser.ast.statement.InsertStatement;
import fdbs.sql.parser.ast.statement.UpdateStatement;
import fdbs.sql.resolver.CreateStatementResolver;
import fdbs.sql.resolver.InsertStatementResolver;
import fdbs.sql.resolver.UpdateStatementResolver;
import fdbs.util.logger.Logger;
import java.sql.ResultSet;
import org.junit.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 * @author patrick
 */
public class InsertStatementResolverTest {

    String[] statements;
    FedConnection fed;
    private TestHelper helper;

    public InsertStatementResolverTest() {
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

    @After
    public void tearDown() {
    }

    /**
     * Test of resolveStatement method, of class InsertStatementResolver.
     */
    private void executeStatements(String[] statements) throws Exception {
        SQLExecuter executer = new SQLExecuter(fed);
        for (String stm : statements) {
            SqlParser parser = new SqlParser(stm);
            AST tree = parser.parseStatement();
            if (tree.getRoot() instanceof UpdateStatement) {
                UpdateStatementResolver r = new UpdateStatementResolver(fed);
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
            "INSERT INTO FLUGHAFEN VALUES ('Auckland International', 'Auckland','NZ','AUI')",};

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 0);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("AUI", rs.getString("FHC"));
        Assert.assertEquals("Auckland International", rs.getString("NAME"));
        Assert.assertEquals("AUI", rs.getString(1));
        Assert.assertEquals("Auckland International", rs.getString(2));

        rs = task.getDb2Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("AUI", rs.getString("FHC"));
        Assert.assertEquals("Auckland", rs.getString("STADT"));
        Assert.assertEquals("AUI", rs.getString(1));
        Assert.assertEquals("Auckland", rs.getString(2));

        rs = task.getDb3Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("AUI", rs.getString("FHC"));
        Assert.assertEquals("NZ", rs.getString("LAND"));
        Assert.assertEquals("NZ", rs.getString(1));
        Assert.assertEquals("AUI", rs.getString(2));
    }
    
   @Test
    public void testResolveStatementVerticalWithTwoDBs() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;
 
        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) vertical((NAME, STADT),(LAND, FHC)) ",
            "INSERT INTO FLUGHAFEN VALUES ('Auckland International', 'Auckland','NZ','AUI')",};

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        
        //verify column value
        Assert.assertEquals("AUI", rs.getString("FHC"));
        Assert.assertEquals("Auckland International", rs.getString("NAME"));
        Assert.assertEquals("Auckland", rs.getString("STADT"));
        
        //verify column order
        Assert.assertEquals("AUI", rs.getString(1));
        Assert.assertEquals("Auckland International", rs.getString(2));
        Assert.assertEquals("Auckland", rs.getString(3));
        
        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 2);
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        rs.next();
        
        Assert.assertEquals("AUI", rs.getString("FHC"));
        Assert.assertEquals("NZ", rs.getString("LAND"));
        Assert.assertEquals("NZ", rs.getString(1));
        Assert.assertEquals("AUI", rs.getString(2));

    }    

    @Test
    public void testResolveStatementHorizontalWithThreeDBs() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal(FHC('MA','MY')) ",
            "INSERT INTO FLUGHAFEN VALUES ('LZZ', 'NZ', 'Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('MBB', 'AU', 'Sydney','Sydney Airport')",
            "INSERT INTO FLUGHAFEN VALUES ('MZZ', 'AU2', 'Sydney2','Sydney Airport2')",
            "INSERT INTO FLUGHAFEN VALUES ('N', 'DE', 'Munich','Munich Airport')",};

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 0);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("LZZ", rs.getString("FHC"));
        Assert.assertEquals("Auckland International", rs.getString("NAME"));

        rs = task.getDb2Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("MBB", rs.getString("FHC"));
        Assert.assertEquals("Sydney Airport", rs.getString("NAME"));

        rs = task.getDb3Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("MZZ", rs.getString("FHC"));
        Assert.assertEquals("Sydney Airport2", rs.getString("NAME"));
        
        rs.next();
        Assert.assertEquals("N", rs.getString("FHC"));
        Assert.assertEquals("Munich Airport", rs.getString("NAME"));
    }

    @Test
    public void testResolveStatementHorizontalWithTwoDBs() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal(FHC('MA')) ",
            "INSERT INTO FLUGHAFEN VALUES ('MA', 'NZ', 'Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('SYD', 'AU', 'Sydney','Sydney Airport')",
            "INSERT INTO FLUGHAFEN VALUES ('MBC', 'DE', 'Munich','Munich Airport')",};

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("MA", rs.getString("FHC"));
        Assert.assertEquals("Auckland International", rs.getString("NAME"));

        task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 2);
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("SYD", rs.getString("FHC"));
        Assert.assertEquals("Sydney Airport", rs.getString("NAME"));

        rs.next();
        Assert.assertEquals("MBC", rs.getString("FHC"));
        Assert.assertEquals("Munich Airport", rs.getString("NAME"));
    }

    @Test
    public void testResolveStatementNormal() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;
        Logger.info("Testing vertical insert statements.");
        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) ",
            "INSERT INTO FLUGHAFEN VALUES ('AUI', 'NZ', 'Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('SYD', 'AU', 'Sydney','Sydney Airport')",
            "INSERT INTO FLUGHAFEN VALUES ('MU', 'DE', 'Munich','Munich Airport')",};

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("AUI", rs.getString("FHC"));
        Assert.assertEquals("Auckland International", rs.getString("NAME"));

        rs.next();
        Assert.assertEquals("SYD", rs.getString("FHC"));
        Assert.assertEquals("Sydney Airport", rs.getString("NAME"));

        rs.next();
        Assert.assertEquals("MU", rs.getString("FHC"));
        Assert.assertEquals("Munich Airport", rs.getString("NAME"));

    }
    
    @Test
    public void testResolveStatementHorizontalWithThreeDBsExplicitBoundary() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal(FHC('MA','MY')) ",
            "INSERT INTO FLUGHAFEN VALUES ('LZZ', 'NZ', 'Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('MA', 'AU', 'Sydney','Sydney Airport')",
            "INSERT INTO FLUGHAFEN VALUES ('MY', 'AU2', 'Sydney2','Sydney Airport2')",
            "INSERT INTO FLUGHAFEN VALUES ('MZ', 'DE', 'Munich','Munich Airport')",};

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 0);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("LZZ", rs.getString("FHC"));
        Assert.assertEquals("Auckland International", rs.getString("NAME"));
        rs.next();
        Assert.assertEquals("MA", rs.getString("FHC"));
        Assert.assertEquals("Sydney Airport", rs.getString("NAME"));
        
        rs = task.getDb2Statements().get(0).getQueryResultSet();
        
        rs.next();
        Assert.assertEquals("MY", rs.getString("FHC"));
        Assert.assertEquals("Sydney Airport2", rs.getString("NAME"));

        rs = task.getDb3Statements().get(0).getQueryResultSet();

        rs.next();
        Assert.assertEquals("MZ", rs.getString("FHC"));
        Assert.assertEquals("Munich Airport", rs.getString("NAME"));
    }    

}
