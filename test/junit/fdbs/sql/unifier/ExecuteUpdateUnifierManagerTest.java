/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.unifier;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedPseudoDriver;
import fdbs.sql.executer.SQLExecuterSubTask;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.unifier.ExecuteUpdateUnifierManager;
import fdbs.util.logger.Logger;
import fdbs.util.statement.StatementUtil;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Markus
 */
public class ExecuteUpdateUnifierManagerTest {
    //String[] statements;
    FedConnection fed;
    MetadataManager manager;
    String[] statements;
    
    public ExecuteUpdateUnifierManagerTest() {
        
        try {
            fed = (new FedPseudoDriver()).getConnection("VDBSA04", "VDBSA04");
            manager = MetadataManager.getInstance(fed);
            manager.deleteMetadata();
            manager.deleteTables();
            manager.checkAndRefreshTables(); 
            try
            {
                statements = StatementUtil.getStatementsFromFiles(new String[]{"test\\junit\\fdbs\\sql\\meta\\CreateQueries.sql"});
                for (String stm : statements) {
                    SqlParser parser = new SqlParser(stm);
                    AST ast = parser.parseStatement();
                    manager.handleQuery(ast);
                }
            }
            catch(IOException e)
            {
                FedException f = new FedException("An Exception has occured");
                f.addSuppressed(e);
            } catch (ParseException ex) {
                java.util.logging.Logger.getLogger(ExecuteUpdateUnifierManagerTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (FedException ex) {
            java.util.logging.Logger.getLogger(ExecuteUpdateUnifierManagerTest.class.getName()).log(Level.SEVERE, null, ex);
        }  
        
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
        Logger.infoln("Start to test ExecuteUpdateManager:");        
    }
    
    @After
    public void tearDown() {
        Logger.infoln("End test of ExecuteUpdateManager");
        Logger.infoln("---------------------------------------------------------------\n");
    }

    // <editor-fold defaultstate="collapsed" desc="Insert Unifier Tests" >
    @Test
    public void testExecuteUpdateUnifierInsertHorizontalWithOne() throws FedException, ParseException, SQLException {
        String stm = "INSERT INTO PASSAGIER VALUES ('1', 'Mueller', 'Bernd', 'GER')";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(1);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(1);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(1);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 1 ", 1 == oneDbValue);
        Assert.assertTrue("unified value should be 1 ", 1 == twoDbValue);
        Assert.assertTrue("unified value should be 1 ", 1 == threeDbValue);

        System.out.println(String.format("empty Array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierInsertHorizontalWithZero() throws FedException, ParseException, SQLException {
        
        String stm = "INSERT INTO PASSAGIER VALUES ('1', 'Mueller', 'Bernd', 'GER')";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(0);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == twoDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == threeDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierInsertVerticalWithOne() throws FedException, ParseException, SQLException {
        
            String stm = "INSERT INTO FLUGHAFEN VALUES ('FRA', 'Germany', 'Frankfurt', 'Frankfurt Airport')";
            SqlParser parser = new SqlParser(stm);
            AST ast = parser.parseStatement();
            
            ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
            SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();
            
            SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
            subTaskDb1.setUpdateResult(1);
            SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
            subTaskDb2.setUpdateResult(1);
            SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
            subTaskDb3.setUpdateResult(1);
            
            int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
            
            sqlExecuterTask.addSubTask(subTaskDb1, 1);
            int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
            sqlExecuterTask.addSubTask(subTaskDb2, 2);
            int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
            sqlExecuterTask.addSubTask(subTaskDb3, 3);
            int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

            Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
            Assert.assertTrue("unified value should be 1 ", 1 == oneDbValue);
            Assert.assertTrue("unified value should be 1 ", 1 == twoDbValue);
            Assert.assertTrue("unified value should be 1 ", 1 == threeDbValue);

            System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
            System.out.println(String.format("One DB array value is %s: ", oneDbValue));
            System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
            System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierInsertVerticalWithOneAndDifferentValues() throws ParseException, SQLException {
        
            String stm = "INSERT INTO FLUGHAFEN VALUES ('FRA', 'Germany', 'Frankfurt', 'Frankfurt Airport')";
            SqlParser parser = new SqlParser(stm);
            AST ast = parser.parseStatement();
            try
            {
                ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
                SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

                SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
                subTaskDb1.setUpdateResult(1);
                SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
                subTaskDb2.setUpdateResult(1);
                SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
                subTaskDb3.setUpdateResult(0);


                int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

                sqlExecuterTask.addSubTask(subTaskDb1, 1);
                int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
                sqlExecuterTask.addSubTask(subTaskDb2, 2);
                int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
                sqlExecuterTask.addSubTask(subTaskDb3, 3);
                
                
                
                Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
                Assert.assertTrue("unified value should be 1 ", 1 == oneDbValue);
                Assert.assertTrue("unified value should be 1 ", 1 == twoDbValue);

                System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
                System.out.println(String.format("One DB array value is %s: ", oneDbValue));
                System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
                
                try
                {    
                    int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
                }
                catch(FedException errorForNotEqualValues){
                    System.out.println(String.format("This is a provoked FedException %s: ", errorForNotEqualValues));
                }
             
            }
            catch(FedException e){
                System.out.println(e);
            }
    }
    
    @Test
    public void testExecuteUpdateUnifierInsertVerticalWithZero() throws FedException, ParseException, SQLException {
        
        String stm = "INSERT INTO PASSAGIER VALUES ('1', 'Mueller', 'Bernd', 'GER')";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(0);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == twoDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == threeDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
        
    @Test
    public void testExecuteUpdateUnifierInsertNoneWithZero() throws FedException, ParseException, SQLException {
        
        String stm = "INSERT INTO STADT VALUES ('1', 'Frankfurt')";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();
        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierInsertNoneWithOne() throws FedException, ParseException, SQLException {
        
        String stm = "INSERT INTO STADT VALUES ('1', 'Frankfurt')";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();
        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(1);
        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 1 ", 1 == oneDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
    }
    // </editor-fold>
   
    // <editor-fold defaultstate="collapsed" desc="Update Unifier Tests" >
    @Test
    public void testExecuteUpdateUnifierUpdateHorizontalWithOne() throws FedException, ParseException, SQLException {
        String stm = "UPDATE PASSAGIER SET LAND = 'DE' WHERE PNR <= 10";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(2);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(5);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 2 ", 2 == oneDbValue);
        Assert.assertTrue("unified value should be 5 ", 5 == twoDbValue);
        Assert.assertTrue("unified value should be 10 ", 10 == threeDbValue);

        System.out.println(String.format("empty Array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierUpdateHorizontalWithZero() throws FedException, ParseException, SQLException {
        String stm = "UPDATE PASSAGIER SET LAND = 'DE' WHERE PNR <= 10";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(5);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == twoDbValue);
        Assert.assertTrue("unified value should be 5 ", 5 == threeDbValue);

        System.out.println(String.format("empty Array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierUpdateVerticalWithAffectedLines() throws FedException, ParseException, SQLException {
        
        String stm = "UPDATE FLUGHAFEN SET NAME = 'Rudirudolf' WHERE FHC = 'FRA' ";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(3);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 3 ", 3 == oneDbValue);
        Assert.assertTrue("unified value should be 3 ", 3 == twoDbValue);
        Assert.assertTrue("unified value should be 3 ", 3 == threeDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierUpdateVerticalWithMoreValuesPerDb() throws FedException, ParseException, SQLException {
        
        String stm = "UPDATE FLUGHAFEN SET NAME = 'Rudirudolf' WHERE FHC = 'FRA' ";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(2);
        SQLExecuterSubTask subTaskDb1_1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1_1.setUpdateResult(1);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb3_1 = new SQLExecuterSubTask(stm, true);
        subTaskDb3_1.setUpdateResult(8);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        sqlExecuterTask.addSubTask(subTaskDb1_1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        sqlExecuterTask.addSubTask(subTaskDb3_1, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 2 ", 2 == oneDbValue);
        Assert.assertTrue("unified value should be 3 ", 3 == twoDbValue);
        Assert.assertTrue("unified value should be 8 ", 8 == threeDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierUpdateVerticalWithoutAffectedLines() throws FedException, ParseException, SQLException {
        
        String stm = "UPDATE FLUGHAFEN SET NAME = 'Rudirudolf' WHERE FHC = 'FRA' ";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(0);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == twoDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == threeDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierUpdateVerticalWithDifferentValues() throws FedException, ParseException, SQLException {
        
        String stm = "UPDATE FLUGHAFEN SET NAME = 'Rudirudolf' WHERE FHC = 'FRA' ";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(2);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);


        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 3 ", 3 == oneDbValue);
        Assert.assertTrue("unified value should be 3 ", 3 == twoDbValue);
        Assert.assertTrue("unified value should be 3 ", 3 == threeDbValue);


        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue)); 
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierUpdateNone() throws FedException, ParseException, SQLException {
        
        String stm = "UPDATE STADT SET NAME = 'Rudirudolf' WHERE SID <= 10 ";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();
        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(10);
        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 10 ", 10 == oneDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
    }
    
    // </editor-fold>
    
    // <editor-fold defaultstate="collapsed" desc="Delete Unifier Tests" >
    @Test
    public void testExecuteUpdateUnifierDeleteNone() throws FedException, ParseException, SQLException {
        String stm = "DELETE FROM BUCHUNG WHERE VON = 'CDG'";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();
        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(6);
        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 6 ", 6 == oneDbValue);

        System.out.println(String.format("empty array value is %s ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s ", oneDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierDeleteVerticalWithValues() throws FedException, ParseException, SQLException {
        
        String stm = "DELETE FROM FLUGHAFEN WHERE VON = 'CDG'";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(5);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(5);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(5);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 5 ", 5 == oneDbValue);
        Assert.assertTrue("unified value should be 5 ", 5 == twoDbValue);
        Assert.assertTrue("unified value should be 5 ", 5 == threeDbValue);

        System.out.println(String.format("empty array value is %s ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierDeleteVerticalWithMoreThanOneValuePerDb() throws FedException, ParseException, SQLException {
        
        String stm = "DELETE FROM FLUGHAFEN WHERE VON = 'CDG'";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(5);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(5);
        SQLExecuterSubTask subTaskDb2_1 = new SQLExecuterSubTask(stm, true);
        subTaskDb2_1.setUpdateResult(54);
        SQLExecuterSubTask subTaskDb2_2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2_2.setUpdateResult(2);
        SQLExecuterSubTask subTaskDb2_3 = new SQLExecuterSubTask(stm, true);
        subTaskDb2_3.setUpdateResult(7);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(5);
        SQLExecuterSubTask subTaskDb3_1 = new SQLExecuterSubTask(stm, true);
        subTaskDb3_1.setUpdateResult(2);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        sqlExecuterTask.addSubTask(subTaskDb2_1, 2);
        sqlExecuterTask.addSubTask(subTaskDb2_2, 2);
        sqlExecuterTask.addSubTask(subTaskDb2_3, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        sqlExecuterTask.addSubTask(subTaskDb3_1, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 5 ", 5 == oneDbValue);
        Assert.assertTrue("unified value should be 54 ", 54 == twoDbValue);
        Assert.assertTrue("unified value should be 54 ", 54 == threeDbValue);

        System.out.println(String.format("empty array value is %s ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s ", threeDbValue));
    }
    
    
    @Test
    public void testExecuteUpdateUnifierDeleteVerticalWithDifferentValues() throws FedException, ParseException, SQLException {
        
        String stm = "DELETE FROM FLUGHAFEN WHERE VON = 'CDG' ";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(2);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(1);
        
        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);


        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 3 ", 3 == oneDbValue);
        Assert.assertTrue("unified value should be 3 ", 3 == twoDbValue);
        Assert.assertTrue("unified value should be 3 ", 3 == threeDbValue);


        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s ", threeDbValue));
                
    }
    
    @Test
    public void testExecuteUpdateUnifierDeleteVerticalWithoutAffectedLines() throws FedException, ParseException, SQLException {

        String stm = "DELETE FROM FLUGHAFEN WHERE VON = 'CDG' ";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(0);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == twoDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == threeDbValue);

        System.out.println(String.format("empty array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierDeleteHorizontalWithZero() throws FedException, ParseException, SQLException {
        String stm = "DELETE FROM PASSAGIER WHERE LAND = 'GB'";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(78);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(5);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 78 ", 78 == twoDbValue);
        Assert.assertTrue("unified value should be 83 ", 83 == threeDbValue);

        System.out.println(String.format("empty Array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierDeleteHorizontalWithValues() throws FedException, ParseException, SQLException {
        String stm = "UPDATE PASSAGIER SET LAND = 'DE' WHERE PNR <= 10";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(92);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(3);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(5);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 92 ", 92 == oneDbValue);
        Assert.assertTrue("unified value should be 95 ", 95 == twoDbValue);
        Assert.assertTrue("unified value should be 100 ", 100 == threeDbValue);

        System.out.println(String.format("empty Array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    // </editor-fold>
    
    // <editor-fold defaultstate="collapsed" desc="Create Unifier Tests" >
    @Test
    public void testExecuteUpdateUnifierCreateOnlyZero() throws FedException, ParseException, SQLException{
    
        String stm = "create table CHEF (CID integer, MID integer, MID2 integer, NAME varchar(50), constraint CHEF_CID primary key(CID), constraint CHEF_FK_MID foreign key (MID) references MITARBEITER(MID), constraint CHEF_FK_MID2 foreign key (MID2) references MITARBEITER(MID))  VERTICAL((NAME), (MID), (CID, MID2))";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(0);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == twoDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == threeDbValue);

        System.out.println(String.format("empty Array value is %s: ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s: ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s: ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s: ", threeDbValue));
    }
    
    @Test
    public void testExecuteUpdateUnifierCreateWithOne() throws FedException, ParseException, SQLException{
    
        String stm = "create table CHEF (CID integer, MID integer, MID2 integer, NAME varchar(50), constraint CHEF_CID primary key(CID), constraint CHEF_FK_MID foreign key (MID) references MITARBEITER(MID), constraint CHEF_FK_MID2 foreign key (MID2) references MITARBEITER(MID))  VERTICAL((NAME), (MID), (CID, MID2))";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(1);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == twoDbValue);
        Assert.assertTrue("unified value should be 1 ", 1 == threeDbValue);


        System.out.println(String.format("empty Array value is %s ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s ", threeDbValue));
        


    }
    
    // </editor-fold>
    
    // <editor-fold defaultstate="collapsed" desc="Drop Unifier Tests" >
    @Test
    public void testExecuteUpdateUnifierDropWithOne() throws FedException, ParseException, SQLException{
    
        String stm = "drop table PASSAGIER cascade constraints";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(1);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(1);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 1 ", 1 == twoDbValue);
        Assert.assertTrue("unified value should be 1 ", 1 == threeDbValue);


        System.out.println(String.format("empty Array value is %s ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s ", threeDbValue));
       
    }
    
    @Test
    public void testExecuteUpdateUnifierDropOnlyZero() throws FedException, ParseException, SQLException{
    
        String stm = "drop table PASSAGIER cascade constraints";
        SqlParser parser = new SqlParser(stm);
        AST ast = parser.parseStatement();

        ExecuteUpdateUnifierManager instance = new ExecuteUpdateUnifierManager(fed);
        SQLExecuterTask sqlExecuterTask = new SQLExecuterTask();

        SQLExecuterSubTask subTaskDb1 = new SQLExecuterSubTask(stm, true);
        subTaskDb1.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb2 = new SQLExecuterSubTask(stm, true);
        subTaskDb2.setUpdateResult(0);
        SQLExecuterSubTask subTaskDb3 = new SQLExecuterSubTask(stm, true);
        subTaskDb3.setUpdateResult(0);

        int emptyMapArrayValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        sqlExecuterTask.addSubTask(subTaskDb1, 1);
        int oneDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb2, 2);
        int twoDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);
        sqlExecuterTask.addSubTask(subTaskDb3, 3);
        int threeDbValue = instance.unifyUpdateResult(ast, sqlExecuterTask);

        Assert.assertTrue("unified value should be 0 ", 0 == emptyMapArrayValue);
        Assert.assertTrue("unified value should be 0 ", 0 == oneDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == twoDbValue);
        Assert.assertTrue("unified value should be 0 ", 0 == threeDbValue);


        System.out.println(String.format("empty Array value is %s ", emptyMapArrayValue));
        System.out.println(String.format("One DB array value is %s ", oneDbValue));
        System.out.println(String.format("Two DB array value is %s ", twoDbValue));
        System.out.println(String.format("Three DB array value is %s ", threeDbValue));
       

    }
    
    // </editor-fold>
}
