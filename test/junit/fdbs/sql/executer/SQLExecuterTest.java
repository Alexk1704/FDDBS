/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.executer;

import fdbs.sql.FedConnection;
import fdbs.sql.FedPseudoDriver;
import fdbs.sql.executer.SQLExecuterTask;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author patrick
 */
public class SQLExecuterTest {
    
    public SQLExecuterTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of executeTask method, of class SQLExecuter.
     */
    @Test
    public void testExecuteTask() throws Exception {
      /*  FedConnection fed = (new FedPseudoDriver()).getConnection("VDBSA05", "VDBSA05");
        SQLExecuter executer= new SQLExecuter(fed); */
//        SQLExecuterTask task = new SQLExecuterTask();
//        SQLExecuterVerySpecificTask subtask = new SQLExecuterVerySpecificTask("",true);
//        task.addSubTask(subtask, 0);
//        
//        
//        Assert.assertTrue("DB1 Subtask count should be 1",task.getDb1Statements().size() == 1);
//        Assert.assertTrue("DB2 Subtask count should be 1",task.getDb2Statements().size() == 1);
//        Assert.assertTrue("DB3 Subtask count should be 1",task.getDb3Statements().size() == 1);
        // TODO review the generated test code and remove the default call to fail.
    }
    
}
