/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.fedresultset.select.AllAttributes;

import fdbs.sql.FedException;
import fdbs.sql.fedresultset.select.nogroupstatement.NoGroupDefaultResultSet;
import fdbs.util.logger.Logger;
import junit.fdbs.sql.fedresultset.FedResultSetTest;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Markus
 */
public class AllAtributesGroupResultSetTest extends AllAttribtuesNoGroupNonJoinTest{
    
    @Before
    @Override
    public void setUp() throws FedException {
        super.setUp();
        Logger.infoln("Start to test select count all table statement result sets.");
    }

    @After
    @Override
    public void tearDown() throws FedException {
        super.tearDown();
        Logger.infoln("Finished to test select count all table statement result sets.");
    }
    
    @Test
    public void test(){
        System.err.println("Test ausgef");
    }
    
//    @Test
//    public void testDefaultResultSet() throws FedException {
//        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR))"));
//        testSelectWithEmptyList(NoGroupDefaultResultSet.class, "");
//        
//        testSelect(true, NoGroupDefaultResultSet.class, "", null);
//    }
    
}
