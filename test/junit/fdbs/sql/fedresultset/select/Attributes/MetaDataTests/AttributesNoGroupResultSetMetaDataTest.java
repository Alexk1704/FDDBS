package junit.fdbs.sql.fedresultset.select.Attributes.MetaDataTests;

import fdbs.sql.FedException;
import fdbs.sql.fedresultset.select.nogroupstatement.*;
import fdbs.util.logger.Logger;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;
import org.junit.Test;

public class AttributesNoGroupResultSetMetaDataTest extends AttributesNoGroupMetaDataTest {

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

    @Test
    public void testMetaDataMetaDataDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupDefaultResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupDefaultResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVonDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupDefaultResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testFlugVonUnqieSelectMetaData(first, NoGroupDefaultResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVonVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testFlugVonUnqieSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVonVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testFlugVonUnqieSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) vertical((FNR),(FLC,FLNR,VON),(NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVonDB21VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC,FLNR),(VON,NACH),(AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testFlugVonUnqieSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVonDB23VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC,FLNR,VON),(NACH, AB, AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testFlugVonUnqieSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVonDB31VerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR,FLC),(FLNR),(VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testFlugVonUnqieSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataVonDB32VerticalThree3DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (VON)) vertical((FNR),(FLC),(FLNR,VON,NACH,AB,AN))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupVerticalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testFlugVonUnqieSelectMetaData(first, NoGroupVerticalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataHorizontalTwoDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(50))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupHorizontalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupHorizontalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataHorizontalThreeDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(FNR(35,65))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupHorizontalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupHorizontalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataHorizontalTwoDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('MM'))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupHorizontalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupHorizontalResultSet.class, attributes, "", null);
            first = false;
        }
    }

    @Test
    public void testMetaDataHorizontalThreeDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUG (FNR integer, FLC varchar(2),FLNR integer, VON varchar(3),NACH varchar(3),AB integer,AN integer,constraint FLUG_PS primary key (FNR)) horizontal(VON('KK','MM'))"));
        for (List<String> attributes : getAttributesVariations()) {
            testSelectWithEmptyList(NoGroupHorizontalResultSet.class, attributes, "");
        }

        boolean first = true;
        for (List<String> attributes : getAttributesVariations()) {
            testSelectMetaData(first, NoGroupHorizontalResultSet.class, attributes, "", null);
            first = false;
        }
    }
}
