package junit.fdbs.sql.fedresultset.select.Attributes.ValueTests;

import fdbs.sql.FedException;
import fdbs.sql.fedresultset.select.nogroupstatement.NoGroupDefaultResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.NoGroupHorizontalResultSet;
import fdbs.sql.fedresultset.select.nogroupstatement.NoGroupVerticalResultSet;
import fdbs.util.logger.Logger;
import java.util.List;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class AttributesNoGroupResultSetValueTest extends AttributesNoGroupValueTest {

    @Before
    @Override
    public void setUp() throws FedException {
        super.setUp();
        Logger.infoln("Start to test select attributes no group statement result sets.");
    }

    @After
    @Override
    public void tearDown() throws FedException {
        super.tearDown();
        Logger.infoln("Finished to test select attributes no group statement result sets.");
    }

    public void testDataVonDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON))"));
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testSelectWithEmptyList(NoGroupDefaultResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testFlugVonUnqieSelectData(first, NoGroupDefaultResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVonVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testFlugVonUnqieSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVonVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testFlugVonUnqieSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON),(NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVonDB21VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testFlugVonUnqieSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVonDB23VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON),(NACH, AB, AN))"));
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testFlugVonUnqieSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVonDB31VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC),(FLNR),(VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testFlugVonUnqieSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataVonDB32VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC),(FLNR,VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithVONPK()) {
            testFlugVonUnqieSelectData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataHorizontalTwoDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(50))"));
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectWithEmptyList(NoGroupHorizontalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectData(first, NoGroupHorizontalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataHorizontalThreeDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(35,65))"));
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectWithEmptyList(NoGroupHorizontalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectData(first, NoGroupHorizontalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataHorizontalTwoDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('MM'))"));
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectWithEmptyList(NoGroupHorizontalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectData(first, NoGroupHorizontalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testDataHorizontalThreeDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('KK','MM'))"));
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectWithEmptyList(NoGroupHorizontalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariationsWithFNRPK()) {
            testSelectData(first, NoGroupHorizontalResultSet.class, attributes, "", null);
            first = false;
        }
    }
}
