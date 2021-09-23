/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.fedresultset.select.join;

import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import java.sql.ResultSet;
import junit.fdbs.sql.fedresultset.FedResultSetTest;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author patrick
 */
public class FedResultSetJoin extends FedResultSetTest {

    public FedResultSetJoin() {
        super();
    }

    public void createTestTables()throws FedException {
        executeUpdate("create table FLUGHAFEN ( FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC) ) VERTICAL ((FHC, LAND), (STADT, NAME))");
        executeUpdate("create table FLUGLINIE ( FLC varchar(2), LAND varchar(3), HUB varchar(3), NAME varchar(30), ALLIANZ varchar(20), constraint FLUGLINIE_PS primary key (FLC), constraint FLUGLINIE_FS_HUB foreign key (HUB) references FLUGHAFEN(FHC), constraint FLUGLINIE_LAND_NN check (LAND is not null), constraint FLUGLINIE_ALLIANZ_CHK check (ALLIANZ != 'BlackList') ) HORIZONTAL (FLC('KK','MM'))");

    }

    public void insertTestData() throws FedException {

        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('LGW', 'GB', 'London', 'Gatwick')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('LHR', 'GB', 'London', 'Heathrow')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('EDI', 'GB', 'Edinburgh', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ERF', 'D', 'Erfurt', 'Flughafen Erfurt')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('FCO', 'I', 'Rom', 'Fiumicino')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('FRA', 'D', 'Frankfurt', 'Rhein-Main')"));

        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('DB', 'D', 'LGW', 'Database Airlines', null)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('DI', 'D', 'LGW', 'Deutsche BA', null)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('FE', 'EM', 'LHR', 'Fly Emirates', null)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('JL', 'J', 'ERF', 'Japan Airlines', 'OneWorld') "));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('LH', 'D', 'FCO', 'Lufthansa', 'Star')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('NH', 'J', 'FCO', 'All Nippon Airways', 'Star')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('UA', 'USA', null, 'United Airlines', 'Star')"));
    }

    @Before
    public void customSetup() throws FedException {
        createTestTables();
        insertTestData();
    }

    @Test
    /* SELECT WHOLE TABLES */
    public void selectWholeTables() throws FedException {
        FedResultSet s = null;

        String query = "SELECT FLUGLINIE.FLC, FLUGLINIE.NAME, FLUGHAFEN.FHC, FLUGHAFEN.STADT FROM FLUGLINIE, FLUGHAFEN WHERE (FLUGHAFEN.FHC = FLUGLINIE.HUB)";
        s = executeQuery(query).getResultSet();
        assertEquals(4, s.getColumnCount());
        assertEquals(true, s.next());
        assertEquals("LGW", s.getString(3));
        assertEquals("London", s.getString(4));
        assertEquals("Database Airlines", s.getString(2));
        assertEquals(true, s.next());
        assertEquals("LGW", s.getString(3));
        assertEquals("Deutsche BA", s.getString(2));
        assertEquals(true, s.next());
        assertEquals("LHR", s.getString(3));
        assertEquals(true, s.next());
        assertEquals("ERF", s.getString(3));
        assertEquals(true, s.next());
        assertEquals("FCO", s.getString(3));
        assertEquals(true, s.next());
        assertEquals("FCO", s.getString(3));
        assertEquals(false, s.next());
    }

    @Test
    /* SELECT ONE CONJUNCTIVE RESTRICTION */
    public void testOneConjunctiveRestriction() throws FedException {
        FedResultSet s = null;

        String query = "SELECT FLUGLINIE.FLC, FLUGLINIE.NAME, FLUGHAFEN.FHC, FLUGHAFEN.STADT FROM FLUGLINIE, FLUGHAFEN WHERE (FLUGHAFEN.FHC = FLUGLINIE.HUB) AND ((FLUGLINIE.HUB='LGW'))";
        s = executeQuery(query).getResultSet();
        assertEquals(4, s.getColumnCount());

        assertEquals(true, s.next());
        assertEquals("LGW", s.getString(3));
        assertEquals(true, s.next());
        assertEquals("LGW", s.getString(3));
        assertEquals(false, s.next());

    }

    /* SELECT TWO CONJUNCTIVE RESTRICTIONS ON ONE TABLE */
    @Test
    public void testTwoConjunctiveRestrictionOnePerTable() throws FedException {
        FedResultSet s = null;

        String query = "SELECT FLUGLINIE.FLC, FLUGLINIE.NAME, FLUGHAFEN.FHC, FLUGHAFEN.STADT FROM FLUGLINIE, FLUGHAFEN WHERE (FLUGHAFEN.FHC = FLUGLINIE.HUB) AND ((FLUGHAFEN.FHC='LGW')AND(FLUGLINIE.NAME='Database Airlines'))";
        s = executeQuery(query).getResultSet();
        assertEquals(4, s.getColumnCount());

        assertEquals(true, s.next());
        assertEquals("LGW", s.getString(3));
        assertEquals(false, s.next());
    }

    /* SELECT TWO CONJUNCTIVE RESTRICTIONS ON ONE TABLE */
    @Test
    public void testTwoConjunctiveRestrictionOnOneTable() throws FedException {
        FedResultSet s = null;

        String query = "SELECT FLUGLINIE.FLC, FLUGLINIE.NAME, FLUGHAFEN.FHC, FLUGHAFEN.STADT FROM FLUGLINIE, FLUGHAFEN WHERE (FLUGHAFEN.FHC = FLUGLINIE.HUB) AND ((FLUGLINIE.HUB='LGW')AND(FLUGLINIE.LAND='D'))";
        s = executeQuery(query).getResultSet();
        assertEquals(4, s.getColumnCount());
        assertEquals(true, s.next());
        assertEquals("LGW", s.getString(3));
        assertEquals(true, s.next());
        assertEquals("LGW", s.getString(3));
        assertEquals(false, s.next());

    }

    /* SELECT ONE DISJUNCTIVE RESTRICTION ON ONE TABLE */
    @Test
    public void testOneDisjunctiveRestrictionOnOneTable() throws FedException {
        FedResultSet s = null;

        String query = "SELECT FLUGLINIE.FLC, FLUGLINIE.NAME, FLUGHAFEN.FHC, FLUGHAFEN.STADT FROM FLUGLINIE, FLUGHAFEN WHERE (FLUGHAFEN.FHC = FLUGLINIE.HUB) AND ((FLUGLINIE.LAND='J')OR(FLUGLINIE.LAND='D'))";
        s = executeQuery(query).getResultSet();
        assertEquals(4, s.getColumnCount());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(false, s.next());
    }

    /* SELECT TWO CONJUNCTIVE RESTRICTIONS ON TWO TABLES */
    @Test
    public void testTwoConjunctiveRestrictionOnTwoTables() throws FedException {
        FedResultSet s = null;

        String query = "SELECT FLUGLINIE.FLC, FLUGLINIE.NAME, FLUGHAFEN.FHC, FLUGHAFEN.STADT FROM FLUGLINIE, FLUGHAFEN WHERE (FLUGHAFEN.FHC = FLUGLINIE.HUB) AND ((FLUGLINIE.LAND='J')AND(FLUGHAFEN.LAND='I'))";
        s = executeQuery(query).getResultSet();
        assertEquals(4, s.getColumnCount());
        assertEquals(true, s.next());
        assertEquals(false, s.next());
    }

    @Test
    public void oneConjunctiveOneDisjunctiveOneTable() throws FedException {
        FedResultSet s = null;

        String query = "SELECT FLUGLINIE.FLC, FLUGLINIE.NAME, FLUGHAFEN.FHC, FLUGHAFEN.STADT FROM FLUGLINIE, FLUGHAFEN WHERE (FLUGHAFEN.FHC = FLUGLINIE.HUB) AND ((FLUGLINIE.LAND='J')OR(FLUGLINIE.LAND='I'))";
        s = executeQuery(query).getResultSet();
        assertEquals(4, s.getColumnCount());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(false, s.next());
    }

    @Test
    public void oneConjunctiveOneDisjunctiveTwoTables() throws FedException {
        FedResultSet s = null;

        String query = "SELECT FLUGLINIE.FLC, FLUGLINIE.NAME, FLUGHAFEN.FHC, FLUGHAFEN.STADT FROM FLUGLINIE, FLUGHAFEN WHERE (FLUGHAFEN.FHC = FLUGLINIE.HUB) AND ((FLUGLINIE.LAND='J')OR(FLUGHAFEN.LAND='GB'))";
        s = executeQuery(query).getResultSet();
        assertEquals(4, s.getColumnCount());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(true, s.next());
        assertEquals(false, s.next());
    }

}
