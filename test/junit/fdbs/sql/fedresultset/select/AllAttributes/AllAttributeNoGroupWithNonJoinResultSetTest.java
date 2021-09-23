package junit.fdbs.sql.fedresultset.select.AllAttributes;

import fdbs.sql.FedException;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin.*;
import fdbs.util.logger.Logger;
import junit.fdbs.sql.fedresultset.select.NonJoinInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class AllAttributeNoGroupWithNonJoinResultSetTest extends AllAttribtuesNoGroupNonJoinTest {

    @Before
    @Override
    public void setUp() throws FedException {
        super.setUp();
        Logger.infoln("Start to test select all attributes no group statement with non join result sets.");
    }

    @After
    @Override
    public void tearDown() throws FedException {
        super.tearDown();
        Logger.infoln("Finished to test select all attributes no group statement with non join result sets.");
    }

    @Test
    public void testDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinDefaultResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinDefaultResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinDefaultResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinDefaultResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON),(NACH,AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDB21VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDB23VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON),(NACH, AB, AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDB31VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC),(FLNR),(VON,NACH,AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testVonDB32VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC),(FLNR,VON,NACH,AB,AN))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testVonUnqieSelect(first, NoGroupWithNonJoinVerticalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testHorizontalTwoDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(50))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinHorizontalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinHorizontalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testHorizontalThreeDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(35,65))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinHorizontalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinHorizontalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testHorizontalTwoDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('MM'))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinHorizontalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinHorizontalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }

    @Test
    public void testHorizontalThreeDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('KK','MM'))"));
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelectWithEmptyList(NoGroupWithNonJoinHorizontalResultSet.class, createWhereClause(info));
        }
        boolean first = true;
        for (NonJoinInfo info : getNonJoinInfos()) {
            testSelect(first, NoGroupWithNonJoinHorizontalResultSet.class, createWhereClause(info), info.getFilterFunction());
            first = false;
        }
    }
}
