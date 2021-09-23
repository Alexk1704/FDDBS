package junit.fdbs.sql.fedresultset.select.AllAttributes;

import fdbs.sql.FedException;
import fdbs.sql.fedresultset.select.nogroupstatement.*;
import fdbs.util.logger.Logger;
import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;
import org.junit.Test;

public class AllAttributeNoGrouptResultSetTest extends AllAttribtuesNoGroupNonJoinTest {

    @Before
    @Override
    public void setUp() throws FedException {
        super.setUp();
        Logger.infoln("Start to test select all attributes no group statement result sets.");
    }

    @After
    @Override
    public void tearDown() throws FedException {
        super.tearDown();
        Logger.infoln("Finished to test select all attributes no group statement result sets.");
    }

    @Test
    public void testDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR))"));
        testSelectWithEmptyList(NoGroupDefaultResultSet.class, "");
        testSelect(true, NoGroupDefaultResultSet.class, "", null);
    }

    @Test
    public void testVonDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON))"));
        testSelectWithEmptyList(NoGroupDefaultResultSet.class, "");
        testVonUnqieSelect(true, NoGroupDefaultResultSet.class, "", null);
    }

    @Test
    public void testVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVonVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testVonUnqieSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVonVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testVonUnqieSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON),(NACH,AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVonDB21VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testVonUnqieSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVonDB23VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON),(NACH, AB, AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testVonUnqieSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVonDB31VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC),(FLNR),(VON,NACH,AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testVonUnqieSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testVonDB32VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC),(FLNR,VON,NACH,AB,AN))"));
        testSelectWithEmptyList(NoGroupVerticalResultSet.class, "");
        testVonUnqieSelect(true, NoGroupVerticalResultSet.class, "", null);
    }

    @Test
    public void testHorizontalTwoDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(50))"));
        testSelectWithEmptyList(NoGroupHorizontalResultSet.class, "");
        testSelect(true, NoGroupHorizontalResultSet.class, "", null);
    }

    @Test
    public void testHorizontalThreeDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(35,65))"));
        testSelectWithEmptyList(NoGroupHorizontalResultSet.class, "");
        testSelect(true, NoGroupHorizontalResultSet.class, "", null);
    }

    @Test
    public void testHorizontalTwoDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('MM'))"));
        testSelectWithEmptyList(NoGroupHorizontalResultSet.class, "");
        testSelect(true, NoGroupHorizontalResultSet.class, "", null);
    }

    @Test
    public void testHorizontalThreeDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('KK','MM'))"));
        testSelectWithEmptyList(NoGroupHorizontalResultSet.class, "");
        testSelect(true, NoGroupHorizontalResultSet.class, "", null);
    }
}
