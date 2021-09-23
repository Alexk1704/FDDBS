package junit.fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedPseudoDriver;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
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
import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author patrick
 */
public class UpdateStatementResolverTest {

    String[] statements;
    FedConnection fed;
    TestHelper helper;

    public UpdateStatementResolverTest() {
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
    public void testVerticalWithWhere() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) vertical((NAME),(STADT),(LAND, FHC)) ",
            "INSERT INTO FLUGHAFEN VALUES ('Auckland International', 'Auckland','NZ','AKL')",
            "INSERT INTO FLUGHAFEN VALUES ('Munich Airport', 'Munich','DE','MU')",            
            "UPDATE FLUGHAFEN SET NAME = null WHERE FHC = 'AKL'",
            "UPDATE FLUGHAFEN SET NAME = 'Munich Airport Updated' WHERE STADT = 'Munich'"            
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 0);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("AKL", rs.getString("FHC"));
        Assert.assertEquals(null, rs.getString("NAME"));

        rs.next();
        Assert.assertEquals("MU", rs.getString("FHC"));
        Assert.assertEquals("Munich Airport Updated", rs.getString("NAME"));

    }       
    
    @Test
    public void testVerticalNoWhere() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) vertical((NAME),(STADT),(LAND, FHC)) ",               
            "INSERT INTO FLUGHAFEN VALUES ('Auckland International', 'Auckland','NZ','AKL')",
            "INSERT INTO FLUGHAFEN VALUES ('Munich Airport', 'Munich','DE','MU')",                 
            "UPDATE FLUGHAFEN SET NAME = 'Wurst'",
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("AKL", rs.getString("FHC"));
        Assert.assertEquals("Wurst", rs.getString("NAME"));

        rs.next();
        Assert.assertEquals("MU", rs.getString("FHC"));
        Assert.assertEquals("Wurst", rs.getString("NAME"));
    }         
    
    @Test
    public void testVerticalNoWhereInt() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC integer, LAND varchar(3), STADT varchar(50), NAME integer, "
            + "constraint FLUGHAFEN_PS primary key (FHC)) vertical((NAME),(STADT),(LAND, FHC)) ",               
            "INSERT INTO FLUGHAFEN VALUES (500, 'Auckland','NZ',1)",
            "INSERT INTO FLUGHAFEN VALUES (600, 'Munich','DE',2)",                 
            "UPDATE FLUGHAFEN SET NAME = 1337",
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 1);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("1", rs.getString("FHC"));
        Assert.assertEquals("1337", rs.getString("NAME"));

        rs.next();
        Assert.assertEquals("2", rs.getString("FHC"));
        Assert.assertEquals("1337", rs.getString("NAME"));
    }    
    
    @Test
    public void testHorizontalNoWhere() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal (STADT('L','R')) ",          

            "INSERT INTO FLUGHAFEN VALUES ('AKL', 'NZ','Auckland','Auckland International')",
            "INSERT INTO FLUGHAFEN VALUES ('MU', 'DE','Munich','Munich International')",
            "INSERT INTO FLUGHAFEN VALUES ('SA', 'DE','Saarbrücken','Saarbrücken International')",
           
         
            "UPDATE FLUGHAFEN SET NAME = 'Wurst'",
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 0);
        ResultSet rs = null;
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("AKL", rs.getString("FHC"));
        Assert.assertEquals("Wurst", rs.getString("NAME"));

        rs = task.getDb2Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("MU", rs.getString("FHC"));
        Assert.assertEquals("Wurst", rs.getString("NAME"));
        
        rs = task.getDb3Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("SA", rs.getString("FHC"));
        Assert.assertEquals("Wurst", rs.getString("NAME"));        

    }          
    
    //@Test
    public void testHorizontalNoWhereUpdateAttributeString() throws Exception {
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
           
            "UPDATE FLUGHAFEN SET NAME = 'Wurst'",
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 0);
        ResultSet rs = null;
  
        rs = task.getDb3Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("SA", rs.getString("FHC"));
        Assert.assertEquals("Wurst", rs.getString("NAME"));        
        rs.next();
        Assert.assertEquals("AKL", rs.getString("FHC"));
        Assert.assertEquals("Wurst", rs.getString("NAME"));        
        rs.next();
        Assert.assertEquals("MU", rs.getString("FHC"));
        Assert.assertEquals("Wurst", rs.getString("NAME"));        

    }       
    
    @Test
    public void testHorizontalNoWhereUpdateAttributeInt() throws Exception {
        setUp();
        helper.resetDatabases();
        helper.resetMetadata();
        String[] testCase = null;

        testCase = new String[]{
            "create table FLUGHAFEN (FHC integer, LAND varchar(3), STADT varchar(50), NAME integer, "
            + "constraint FLUGHAFEN_PS primary key (FHC)) horizontal (NAME(500,1000)) ",          

            "INSERT INTO FLUGHAFEN VALUES (2, 'NZ','Auckland',100)",
            "INSERT INTO FLUGHAFEN VALUES (3, 'DE','Munich',501)",
            "INSERT INTO FLUGHAFEN VALUES (4, 'DE','Saarbrücken',1001)",
           
            "UPDATE FLUGHAFEN SET NAME = 35",
        };

        executeStatements(testCase);

        SQLExecuterTask task = helper.executeSelect("SELECT * FROM FLUGHAFEN", 0);
        ResultSet rs = null;
  
        rs = task.getDb1Statements().get(0).getQueryResultSet();
        rs.next();
        Assert.assertEquals("2", rs.getString("FHC"));
        Assert.assertEquals(35, rs.getInt("NAME"));        
        rs.next();
        Assert.assertEquals("3", rs.getString("FHC"));
        Assert.assertEquals(35, rs.getInt("NAME"));        
        rs.next();
        Assert.assertEquals("4", rs.getString("FHC"));
        Assert.assertEquals(35, rs.getInt("NAME"));        

    }     

}
