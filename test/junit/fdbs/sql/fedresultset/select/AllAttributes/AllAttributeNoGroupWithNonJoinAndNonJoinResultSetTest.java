package junit.fdbs.sql.fedresultset.select.AllAttributes;

import fdbs.sql.FedException;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinandnonjoin.*;
import fdbs.util.logger.Logger;
import junit.fdbs.sql.fedresultset.select.NestedNonJoinInfos;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class AllAttributeNoGroupWithNonJoinAndNonJoinResultSetTest extends AllAttribtuesNoGroupNonJoinTest {

    @Before
    @Override
    public void setUp() throws FedException {
        super.setUp();
        Logger.infoln("Start to test select all attributes no group with non join and non join where result sets.");
    }

    @After
    @Override
    public void tearDown() throws FedException {
        super.tearDown();
        Logger.infoln("Finished to test select all attributes no group with non join and non join where result sets.");
    }

    @Test
    public void testDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinDefaultResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinAndNonJoinDefaultResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinDefaultResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinAndNonJoinDefaultResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
               testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
           testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
           testVonUnqieSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
             testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
           testSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON),(NACH,AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDB21VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDB23VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON),(NACH, AB, AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDB31VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC),(FLNR),(VON,NACH,AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDB32VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC),(FLNR,VON,NACH,AB,AN))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinAndNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testHorizontalTwoDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(50))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinHorizontalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinAndNonJoinHorizontalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testHorizontalThreeDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(35,65))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinHorizontalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinAndNonJoinHorizontalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testHorizontalTwoDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('MM'))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinHorizontalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinAndNonJoinHorizontalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testHorizontalThreeDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('KK','MM'))"));
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinAndNonJoinHorizontalResultSet.class, createWhereClause(info));
        }

        boolean first = true;
        for (NestedNonJoinInfos info : getNestedAndNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinAndNonJoinHorizontalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

}
