package junit.fdbs.sql.fedresultset.select.Attributes.ValueTests;

import fdbs.sql.FedException;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoin.*;
import fdbs.util.logger.Logger;
import java.util.List;
import junit.fdbs.sql.fedresultset.select.AllAttributes.AllAttribtuesNoGroupNonJoinTest;
import junit.fdbs.sql.fedresultset.select.NonJoinInfo;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class AttributesNoGroupWithNonJoinResultSetValueTest extends AttributesNoGroupValueTest {

    @Before
    @Override
    public void setUp() throws FedException {
        super.setUp();
        Logger.infoln("Start to test select attributes no group statement with non join where result sets.");
    }

    @After
    @Override
    public void tearDown() throws FedException {
        super.tearDown();
        Logger.infoln("Finished to test select attributes no group statement with non join where result sets.");
    }

    @Test
    public void testDataVonDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinDefaultResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testFlugVonUnqieSelectData(first, NoGroupWithNonJoinDefaultResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVonVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testFlugVonUnqieSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVonVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testFlugVonUnqieSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON),(NACH,AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVonDB21VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testFlugVonUnqieSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVonDB23VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON),(NACH, AB, AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testFlugVonUnqieSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVonDB31VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC),(FLNR),(VON,NACH,AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testFlugVonUnqieSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataVonDB32VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC),(FLNR,VON,NACH,AB,AN))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithVONPK()) {
                testFlugVonUnqieSelectData(first, NoGroupWithNonJoinVerticalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataHorizontalTwoDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(50))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinHorizontalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectData(first, NoGroupWithNonJoinHorizontalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataHorizontalThreeDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(35,65))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinHorizontalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectData(first, NoGroupWithNonJoinHorizontalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataHorizontalTwoDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('MM'))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinHorizontalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectData(first, NoGroupWithNonJoinHorizontalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testDataHorizontalThreeDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('KK','MM'))"));
        boolean first = true;
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectWithEmptyList(NoGroupWithNonJoinHorizontalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NonJoinInfo info : AllAttribtuesNoGroupNonJoinTest.getNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
                testSelectData(first, NoGroupWithNonJoinHorizontalResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }
}
