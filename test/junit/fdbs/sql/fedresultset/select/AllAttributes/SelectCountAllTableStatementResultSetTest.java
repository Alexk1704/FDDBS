package junit.fdbs.sql.fedresultset.select.AllAttributes;

import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.FedStatement;
import fdbs.sql.fedresultset.select.countalltablestatement.*;
import java.sql.Types;
import fdbs.util.logger.Logger;
import junit.fdbs.sql.fedresultset.FedResultSetTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class SelectCountAllTableStatementResultSetTest extends FedResultSetTest {

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
    public void testDefaultResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC))"));
        testSelectOnEmptyList(SelectCountAllTableStatementDefaultResultSet.class);
        testSelect(SelectCountAllTableStatementDefaultResultSet.class);
    }

    @Test
    public void testVerticalTwoDB1ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC)) vertical((FHC,LAND),(STADT),(NAME))"));
        testSelectOnEmptyList(SelectCountAllTableStatementVerticalResultSet.class);
        testSelect(SelectCountAllTableStatementVerticalResultSet.class);
    }

    @Test
    public void testVerticalTwoDB2ResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC)) vertical((FHC),(LAND,STADT),(NAME))"));
        testSelectOnEmptyList(SelectCountAllTableStatementVerticalResultSet.class);
        testSelect(SelectCountAllTableStatementVerticalResultSet.class);
    }

    @Test
    public void testVerticalThreeDBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC)) vertical((FHC,LAND),(STADT),(NAME))"));
        testSelectOnEmptyList(SelectCountAllTableStatementVerticalResultSet.class);
        testSelect(SelectCountAllTableStatementVerticalResultSet.class);
    }

    @Test
    public void testVerticalThree2DBResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC)) vertical((FHC), (LAND,STADT),(NAME))"));
        testSelectOnEmptyList(SelectCountAllTableStatementVerticalResultSet.class);
        testSelect(SelectCountAllTableStatementVerticalResultSet.class);
    }

    @Test
    public void testHorizontalTwoDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC)) horizontal(FHC('MM'))"));
        testSelectOnEmptyList(SelectCountAllTableStatementHorizontalResultSet.class);
        testSelect(SelectCountAllTableStatementHorizontalResultSet.class);
    }

    @Test
    public void testHorizontalThreeDBPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC)) horizontal(FHC('KK','MM'))"));
        testSelectOnEmptyList(SelectCountAllTableStatementHorizontalResultSet.class);
        testSelect(SelectCountAllTableStatementHorizontalResultSet.class);
    }

    @Test
    public void testHorizontalTwoDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC)) horizontal(NAME('MM'))"));
        testSelectOnEmptyList(SelectCountAllTableStatementHorizontalResultSet.class);
        testSelect(SelectCountAllTableStatementHorizontalResultSet.class);
    }

    @Test
    public void testHorizontalThreeDBNotPrimaryKeyResultSet() throws FedException {
        assertEquals(0, executeUpdate("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC)) horizontal(NAME('KK','MM'))"));
        testSelectOnEmptyList(SelectCountAllTableStatementHorizontalResultSet.class);
        testSelect(SelectCountAllTableStatementHorizontalResultSet.class);
    }

    private void testSelect(Class c) throws FedException {
        insertFLUGHAFEN();
        try (FedStatement statement = executeQuery("SELECT COUNT(*) FROM FLUGHAFEN");
                FedResultSet rs = statement.getResultSet();) {
            assertTrue(c.isInstance(rs));
            assertTrue(rs.next());
            assertEquals("84", rs.getString(1));
            assertEquals(84, rs.getInt(1));
            assertEquals(1, rs.getColumnCount());
            assertEquals("COUNT(*)", rs.getColumnName(1));
            assertEquals(Types.NUMERIC, rs.getColumnType(1));

            try {
                rs.getString(0);
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getInt(2);
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getColumnName(0);
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getColumnType(2);
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            assertTrue(!rs.next()); // rs.next() == false

            try {
                rs.getString(1);
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getInt(1);
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getColumnName(1);
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getColumnType(1);
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }
        }
    }

    private void testSelectOnEmptyList(Class c) throws FedException {
        try (FedStatement statement = executeQuery("SELECT COUNT(*) FROM FLUGHAFEN");
                FedResultSet rs = statement.getResultSet();) {
            assertTrue(c.isInstance(rs));
            assertEquals(1, rs.getColumnCount());
            assertEquals("COUNT(*)", rs.getColumnName(1));
            assertEquals(Types.NUMERIC, rs.getColumnType(1));
            
            assertTrue(rs.next());
            assertEquals("0", rs.getString(1));
            assertEquals(0, rs.getInt(1));

            try {
                rs.getString(0);
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getInt(2);
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getColumnName(0);
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getColumnType(2);
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            assertTrue(!rs.next()); // rs.next() == false

            try {
                rs.getString(1);
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getInt(1);
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getColumnName(1);
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getColumnType(1);
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }
        }
    }
}
