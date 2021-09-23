
package junit.fdbs.sql.fedresultset.select.Attributes.MetaDataTests;

import fdbs.sql.FedException;
import fdbs.sql.fedresultset.select.nogroupstatement.where.nonjoinornonjoin.*;
import fdbs.util.logger.Logger;
import java.util.List;
import junit.fdbs.sql.fedresultset.select.AllAttributes.AllAttribtuesNoGroupNonJoinTest;
import static junit.fdbs.sql.fedresultset.select.Attributes.MetaDataTests.AttributesNoGroupMetaDataTest.getAttributesVariations;
import junit.fdbs.sql.fedresultset.select.NestedNonJoinInfos;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class AttributesNoGroupWithNonJoinOrNonJoinResultSetMetaDataTest extends AttributesNoGroupMetaDataTest {

    @Before
    @Override
    public void setUp() throws FedException {
        super.setUp();
        Logger.infoln("Start to test select attributes no group statement with non join or non join where result sets.");
    }

    @After
    @Override
    public void tearDown() throws FedException {
        super.tearDown();
        Logger.infoln("Finished to test select attributes no group statement with non join or non join where result sets.");
    }

    @Test
    public void testMetaDataDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinDefaultResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinDefaultResultSet.class, attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVonDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinDefaultResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testFlugVonUnqieSelectMetaData(first, NoGroupWithNonJoinOrNonJoinDefaultResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVonVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testFlugVonUnqieSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVonVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testFlugVonUnqieSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON),(NACH,AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVonDB21VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testFlugVonUnqieSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVonDB23VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON),(NACH, AB, AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testFlugVonUnqieSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVonDB31VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC),(FLNR),(VON,NACH,AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testFlugVonUnqieSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataVonDB32VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC),(FLNR,VON,NACH,AB,AN))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));
            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testFlugVonUnqieSelectMetaData(first, NoGroupWithNonJoinOrNonJoinVerticalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataHorizontalTwoDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(50))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinHorizontalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));

            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinHorizontalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataHorizontalThreeDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(35,65))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinHorizontalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));

            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinHorizontalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataHorizontalTwoDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('MM'))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinHorizontalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));

            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinHorizontalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }

    @Test
    public void testMetaDataHorizontalThreeDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('KK','MM'))"));
        boolean first = true;
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectWithEmptyList(NoGroupWithNonJoinOrNonJoinHorizontalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info));

            }
        }
        for (NestedNonJoinInfos info : AllAttribtuesNoGroupNonJoinTest.getNestedOrNonJoinInfos()) {
            for (List<String> attributes : getAttributesVariations()) {
                testSelectMetaData(first, NoGroupWithNonJoinOrNonJoinHorizontalResultSet.class,
                        attributes, AllAttribtuesNoGroupNonJoinTest.createWhereClause(info), info.getFilterFunction());
                first = false;
            }
        }
    }
}
