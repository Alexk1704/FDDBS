package junit.fdbs.sql.fedresultset.select.AllAttributes;

import fdbs.sql.*;
import fdbs.sql.meta.MetadataManager;
import fdbs.util.DatabaseCursorChecker;
import fdbs.util.logger.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import junit.fdbs.sql.fedresultset.FedResultSetGroupByTest;
import junit.fdbs.sql.fedresultset.select.*;

import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

public class GroupByTest extends FedResultSetGroupByTest {

    HashMap<Integer, HashMap<Integer, GroupByResults>> testResultsForFiles = new HashMap<>();
    HashMap<Integer, GroupByResults> testResults = new HashMap<>();
    private HashMap<Integer, HashMap<Integer, String>> preparedSelectStmts;

    @Override
    @Before
    public void setUp() throws FedException, SQLException {
        super.setUp();

        createRefTables();
        insertFLUGHAFEN(fedConnectionRef);
        inserFLUGLINIE(fedConnectionRef);
        insertPassengers(fedConnectionRef);
        insertFLUG(fedConnectionRef);
        inserBUCHUNG(fedConnectionRef);

        for (int i = 1; i <= 10; i++) {
            testResultsForFiles.put(i, new HashMap<>());
        }

        preparedSelectStmts = prepareSelectStmts();
        concatTestData(preparedSelectStmts, 1, 3);
        concatTestData(preparedSelectStmts, 2, 3);
        concatTestData(preparedSelectStmts, 3, 6);
        concatTestData(preparedSelectStmts, 4, 3);
        concatTestData(preparedSelectStmts, 5, 2);
        concatTestData(preparedSelectStmts, 6, 4);
        concatTestData(preparedSelectStmts, 7, 3);
        concatTestData(preparedSelectStmts, 8, 4);
        concatTestData(preparedSelectStmts, 9, 4);
        concatTestData(preparedSelectStmts, 10, 4);

        // refData collected
        fedConnectionTest = new FedConnection("VDBSA01", "VDBSA01");
        metadataManagerTest = MetadataManager.getInstance(fedConnectionTest, true);

        createTestTables();

        insertFLUGHAFEN(fedConnectionTest);
        inserFLUGLINIE(fedConnectionTest);
        insertPassengers(fedConnectionTest);
        insertFLUG(fedConnectionTest);
        inserBUCHUNG(fedConnectionTest);
    }

    public GroupByTest() {

    }

    private ResultSet getRealTestDataForStmt(String stmt) throws FedException {
        ResultSet resultSet = null;
        Connection dbConnection = super.fedConnectionRef.getConnections().get(1);
        try {
            java.sql.Statement selectStatement = dbConnection.createStatement();
            resultSet = selectStatement.executeQuery(stmt);
            DatabaseCursorChecker.openStatement(this.getClass());
            DatabaseCursorChecker.openResultSet(this.getClass());
        } catch (SQLException ex) {
            FedException fedException = new FedException(
                    String.format("%s: An error occurred while retrieving local result set for database \"%s\".%n%s",
                            this.getClass().getSimpleName(), 1, ex.getMessage()));

            java.sql.Statement selectStatement = null;
            try {
                if (resultSet != null) {
                    selectStatement = resultSet.getStatement();
                    resultSet.close();
                    DatabaseCursorChecker.closeResultSet(this.getClass());
                }
            } catch (SQLException ex2) {
                //ignore exception
            }
            try {
                if (selectStatement != null) {
                    selectStatement.close();
                    DatabaseCursorChecker.closeStatement(this.getClass());
                }
            } catch (SQLException ex2) {
                //ignore exception
            }
            throw fedException;
        }
        return resultSet;
    }

    private void concatTestData(HashMap<Integer, HashMap<Integer, String>> stmts, int docNum, int amount)
            throws FedException, SQLException {

        for (int i = 1; i <= amount; i++) {

            try (ResultSet tempRs = getRealTestDataForStmt(stmts.get(docNum).get(i));) {
                //Key
                int tempKeyType = tempRs.getMetaData().getColumnType(1);
                //Value
                int tempValueType = tempRs.getMetaData().getColumnType(2);
                GroupByResults rs = new GroupByResults(tempKeyType, tempValueType, tempRs.getMetaData().getColumnName(1),
                        tempRs.getMetaData().getColumnName(2));
                while (tempRs.next()) {
                    rs.increaseAmountRows();
                    if (tempKeyType == 4) {
                        //Case int
                        int tempRowKey = tempRs.getInt(1);
                        int tempRowValue = tempRs.getInt(2);
                        rs.setIntegerRowResult(tempRowKey, tempRowValue);
                    } else {
                        //Case vchar
                        String tempRowKey = tempRs.getString(1);
                        int tempRowValue = tempRs.getInt(2);
                        rs.setStringRowResult(tempRowKey, tempRowValue);
                    }

                }
                testResultsForFiles.get(docNum).put(i, rs);
            }
        }
    }

    private void testAll(final GroupByResults refRs, final FedResultSet testRs) throws FedException {
        //check for row countsx
        assertTrue("FedResultSet Column count is invalid, it should be 2", 2 == testRs.getColumnCount());
        //check for left attribute name
        assertTrue(refRs.getLeftAttributeName().equalsIgnoreCase(testRs.getColumnName(1)));
        //check for right attribute name
        assertTrue(refRs.getRightAttributeName().equalsIgnoreCase(testRs.getColumnName(2)));
        //check left type
        assertTrue(refRs.getLeftType() == (testRs.getColumnType(1)));
        //check right type
        assertTrue(refRs.getRightType() == (testRs.getColumnType(2)));
        int fedResultRowCount = 0;
        while (testRs.next()) {
            fedResultRowCount++;
            //Check each Row
            //4 int , 12 vchar
            if (refRs.getLeftType() == 4) {
                //Check Type
                assertTrue(refRs.containsIntegerRowKey(testRs.getInt(1)));
                //Check Value to Type
                assertTrue(testRs.getInt(2) == refRs.getIntegerRowResult(testRs.getInt(1)));
            } else {
                assertTrue(refRs.containsStringRowKey(testRs.getString(1)));
                assertTrue(testRs.getInt(2) == refRs.getStringRowResult(testRs.getString(1)));
            }
        }
        assertTrue(fedResultRowCount == refRs.getAmountRows());
    }

    private HashMap<Integer, HashMap<Integer, String>> prepareSelectStmts() {
        HashMap<Integer, HashMap<Integer, String>> stringStmts = new HashMap<>();
        HashMap<Integer, String> temp1 = new HashMap<>();
        HashMap<Integer, String> temp2 = new HashMap<>();
        HashMap<Integer, String> temp3 = new HashMap<>();
        HashMap<Integer, String> temp4 = new HashMap<>();
        HashMap<Integer, String> temp5 = new HashMap<>();
        HashMap<Integer, String> temp6 = new HashMap<>();
        HashMap<Integer, String> temp7 = new HashMap<>();
        HashMap<Integer, String> temp8 = new HashMap<>();
        HashMap<Integer, String> temp9 = new HashMap<>();
        HashMap<Integer, String> temp10 = new HashMap<>();
        stringStmts.put(1, temp1);
        stringStmts.put(2, temp2);
        stringStmts.put(3, temp3);
        stringStmts.put(4, temp4);
        stringStmts.put(5, temp5);
        stringStmts.put(6, temp6);
        stringStmts.put(7, temp7);
        stringStmts.put(8, temp8);
        stringStmts.put(9, temp9);
        stringStmts.put(10, temp10);

        stringStmts.get(1).put(1, "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE GROUP BY FLUGLINIE.LAND");
        stringStmts.get(1).put(2, "SELECT BUCHUNG.NACH, SUM(BUCHUNG.MEILEN) FROM BUCHUNG "
                + "GROUP BY BUCHUNG.NACH HAVING (COUNT (*) = 4)");
        stringStmts.get(1).put(3, "SELECT BUCHUNG.VON, SUM(BUCHUNG.PREIS) FROM BUCHUNG WHERE ((BUCHUNG.BNR > 20)) "
                + "GROUP BY BUCHUNG.VON HAVING (COUNT (*) > 10)");

        stringStmts.get(2).put(1, "SELECT FLUG.FLC, COUNT(*) FROM FLUG GROUP BY FLUG.FLC");
        stringStmts.get(2).put(2, "SELECT FLUG.FLC, SUM(FLUG.FNR) FROM FLUG GROUP BY FLUG.FLC");
        stringStmts.get(2).put(3, "SELECT FLUG.FLC, COUNT(*) FROM FLUG GROUP BY FLUG.FLC");

        stringStmts.get(3).put(1, "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE GROUP BY FLUGLINIE.LAND");
        stringStmts.get(3).put(2, "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE "
                + "WHERE ((FLUGLINIE.ALLIANZ = 'Star')) GROUP BY FLUGLINIE.LAND");
        stringStmts.get(3).put(3, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM BUCHUNG "
                + "WHERE ((BUCHUNG.MEILEN > 3000)) GROUP BY BUCHUNG.FLC");
        stringStmts.get(3).put(4, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM BUCHUNG "
                + "WHERE ((BUCHUNG.MEILEN > 3000) AND (BUCHUNG.MEILEN < 4000)) "
                + "GROUP BY BUCHUNG.FLC");
        stringStmts.get(3).put(5, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM BUCHUNG "
                + "WHERE ((BUCHUNG.PNR = 1) OR (BUCHUNG.PNR = 9)) GROUP BY BUCHUNG.FLC");
        stringStmts.get(3).put(6, "SELECT BUCHUNG.PNR, SUM(BUCHUNG.MEILEN) FROM BUCHUNG "
                + "WHERE ((BUCHUNG.MEILEN < 0)) GROUP BY BUCHUNG.PNR");

        stringStmts.get(4).put(1,
                "SELECT BUCHUNG.NACH, COUNT(*) FROM BUCHUNG GROUP BY BUCHUNG.NACH HAVING (COUNT (*) = "
                + "4)");
        stringStmts.get(4).put(2, "SELECT BUCHUNG.VON, COUNT(*) FROM BUCHUNG WHERE (BUCHUNG.BNR > 20) GROUP BY BUCHUNG"
                + ".VON HAVING (COUNT (*) > 10)");
        stringStmts.get(4).put(3,
                "SELECT PASSAGIER.LAND, COUNT(*) FROM PASSAGIER GROUP BY PASSAGIER.LAND HAVING (COUNT"
                + "(*) > 24)");

        stringStmts.get(5).put(1, "SELECT BUCHUNG.NACH, SUM(BUCHUNG.MEILEN) FROM BUCHUNG GROUP BY BUCHUNG.NACH HAVING "
                + "(COUNT (*) = 4)");
        stringStmts.get(5).put(2, "SELECT BUCHUNG.VON, SUM(BUCHUNG.PREIS) FROM BUCHUNG WHERE ((BUCHUNG.PNR < 1000)) "
                + "GROUP BY BUCHUNG.VON HAVING (COUNT (*) > 6)");

        stringStmts.get(6).put(1, "SELECT BUCHUNG.FLC, COUNT(*) FROM BUCHUNG GROUP BY BUCHUNG.FLC");
        stringStmts.get(6).put(2, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.MEILEN) FROM BUCHUNG GROUP BY BUCHUNG.FLC");
        stringStmts.get(6).put(3, "SELECT PASSAGIER.LAND, COUNT(*) FROM PASSAGIER GROUP BY PASSAGIER.LAND");
        stringStmts.get(6).put(4, "SELECT PASSAGIER.GEBORT, COUNT(*) FROM PASSAGIER GROUP BY PASSAGIER.GEBORT");

        stringStmts.get(7).put(1, "SELECT FLUG.FLC, COUNT(*) FROM FLUG GROUP BY FLUG.FLC");
        stringStmts.get(7).put(2, "SELECT FLUG.FLC, SUM(FLUG.FNR) FROM FLUG GROUP BY FLUG.FLC");
        stringStmts.get(7).put(3, "SELECT FLUG.FLC, MAX(FLUG.FNR) FROM FLUG GROUP BY FLUG.FLC");

        stringStmts.get(8).put(1, "SELECT FLUGLINIE.ALLIANZ, COUNT(*) FROM FLUGLINIE GROUP BY FLUGLINIE.ALLIANZ");
        stringStmts.get(8).put(2, "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE WHERE ((FLUGLINIE.ALLIANZ = 'Star')) "
                + "GROUP BY FLUGLINIE.LAND");
        stringStmts.get(8).put(3, "SELECT BUCHUNG.NACH, COUNT(*) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN > 4100) AND "
                + "(BUCHUNG.BNR > 100950)) GROUP BY BUCHUNG.NACH");
        stringStmts.get(8).put(4,
                "SELECT BUCHUNG.VON, COUNT(*) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN > 16000) OR (BUCHUNG"
                + ".BNR < 10)) GROUP BY BUCHUNG.VON");

        stringStmts.get(9).put(1, "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE WHERE ((ALLIANZ = 'Star')) GROUP BY "
                + "FLUGLINIE.LAND");
        stringStmts.get(9).put(2, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN > 3000)) "
                + "GROUP BY BUCHUNG.FLC");
        stringStmts.get(9).put(3, "SELECT BUCHUNG.FLC, MAX(BUCHUNG.PREIS) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN > 3000)) "
                + "GROUP BY BUCHUNG.FLC");
        stringStmts.get(9).put(4, "SELECT BUCHUNG.PNR, SUM(BUCHUNG.MEILEN) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN < 0)) "
                + "GROUP BY BUCHUNG.PNR");
        stringStmts.get(10).put(1,
                "SELECT BUCHUNG.FLC, MAX(BUCHUNG.PNR) FROM PASSAGIER, BUCHUNG WHERE (PASSAGIER.PNR = "
                + "BUCHUNG.PNR) GROUP BY FLC");
        stringStmts.get(10).put(2, "SELECT BUCHUNG.FLC, COUNT(*) FROM PASSAGIER, BUCHUNG WHERE (PASSAGIER.PNR = BUCHUNG"
                + ".PNR) AND ((BUCHUNG.VON = 'FRA') AND (BUCHUNG.NACH = 'CDG')) GROUP BY FLC");
        stringStmts.get(10).put(3,
                "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM PASSAGIER, BUCHUNG WHERE (PASSAGIER.PNR "
                + "= BUCHUNG.PNR) "
                + "AND ((BUCHUNG.BNR = 13020) OR (BUCHUNG.BNR = 13201)) GROUP BY FLC");
        stringStmts.get(10).put(4, "SELECT BUCHUNG.FLC, COUNT(*) FROM PASSAGIER, BUCHUNG WHERE (PASSAGIER.PNR = BUCHUNG"
                + ".PNR) AND ((PASSAGIER.GEBDATINT > 14000)) GROUP BY FLC");

        return stringStmts;
    }

    private void createRefTables() throws FedException {
        List<String> stmts = Arrays.asList("create table FLUGHAFEN ( FHC		varchar(3), LAND		varchar(3), STADT		varchar(50), NAME		varchar(50), constraint FLUGHAFEN_PS 		primary key (FHC) )",
                "create table FLUGLINIE ( FLC		varchar(2), LAND		varchar(3), HUB		varchar(3), NAME		varchar(30), ALLIANZ		varchar(20), constraint FLUGLINIE_PS 		primary key (FLC), constraint FLUGLINIE_FS_HUB 		foreign key (HUB) references FLUGHAFEN(FHC), constraint FLUGLINIE_LAND_NN 		check (LAND is not null), constraint FLUGLINIE_ALLIANZ_CHK 		check (ALLIANZ != 'BlackList') )",
                "create table FLUG ( FNR             integer, FLC		varchar(2), FLNR		integer,		 VON		varchar(3), NACH		varchar(3), AB		integer, AN		integer, constraint FLUG_PS 		primary key (FNR), constraint FLUG_FS_FLC 		foreign key (FLC) references FLUGLINIE(FLC), constraint FLUG_FS_VON 		foreign key (VON) references FLUGHAFEN(FHC), constraint FLUG_FS_NACH 		foreign key (NACH) references FLUGHAFEN(FHC), constraint FLUG_VON_NN 		check (VON is not null), constraint FLUG_NACH_NN 		check (NACH is not null), constraint FLUG_AB_NN 		check (AB is not null), constraint FLUG_AN_NN 		check (AN is not null), constraint FLUG_AB_CHK 		check (AB between 0 and 2400), constraint FLUG_AN_CHK 		check (AN between 0 and 2400), constraint FLUG_VONNACH_CHK 		check (VON != NACH) )",
                "create table PASSAGIER ( PNR		integer, NAME		varchar(40), VORNAME		varchar(40),		 LAND		varchar(3), GEBORT      varchar(50), GEBDAT      varchar(20), GEBDATINT   integer,        constraint PASSAGIER_PS 		primary key (PNR), constraint PASSAGIER_NAME_NN         check (NAME is not null) )",
                "create table BUCHUNG ( BNR             integer, PNR		integer, FLC		varchar(2), FLNR		integer,		 VON		varchar(3), NACH		varchar(3), TAG		varchar(20), MEILEN          integer, PREIS           integer, TAGINT		integer, constraint BUCHUNG_PS 		primary key (BNR), constraint BUCHUNG_FS_PNR 		foreign key (PNR) references PASSAGIER(PNR), constraint BUCHUNG_FS_FLC 		foreign key (FLC) references FLUGLINIE(FLC), constraint BUCHUNG_FS_VON 		foreign key (VON) references FLUGHAFEN(FHC), constraint BUCHUNG_FS_NACH 		foreign key (NACH) references FLUGHAFEN(FHC), constraint BUCHUNG_NACH_NN 		check (NACH is not null), constraint BUCHUNG_MEILEN_NN 		check (MEILEN is not null), constraint BUCHUNG_PREIS_NN 		check (PREIS is not null), constraint BUCHUNG_MEILEN_CHK                 check (MEILEN >= 0), constraint BUCHUNG_PREIS_CHK                 check (PREIS > 0))");

        for (String stmt : stmts) {
            assertEquals(0, executeUpdate(fedConnectionRef, stmt));
        }
    }

    private void createTestTables() throws FedException {
        try {
            List<String> stmts = Arrays.asList("create table FLUGHAFEN ( FHC		varchar(3), LAND		varchar(3), STADT		varchar(50), NAME		varchar(50), constraint FLUGHAFEN_PS 		primary key (FHC) ) VERTICAL ((FHC, LAND), (STADT, NAME))",
                    "create table FLUGLINIE ( FLC		varchar(2), LAND		varchar(3), HUB		varchar(3), NAME		varchar(30), ALLIANZ		varchar(20), constraint FLUGLINIE_PS 		primary key (FLC), constraint FLUGLINIE_FS_HUB 		foreign key (HUB) references FLUGHAFEN(FHC), constraint FLUGLINIE_LAND_NN 		check (LAND is not null), constraint FLUGLINIE_ALLIANZ_CHK 		check (ALLIANZ != 'BlackList') ) HORIZONTAL (FLC('KK','MM'))",
                    "create table FLUG ( FNR             integer, FLC		varchar(2), FLNR		integer,		 VON		varchar(3), NACH		varchar(3), AB		integer, AN		integer, constraint FLUG_PS 		primary key (FNR), constraint FLUG_FS_FLC 		foreign key (FLC) references FLUGLINIE(FLC), constraint FLUG_FS_VON 		foreign key (VON) references FLUGHAFEN(FHC), constraint FLUG_FS_NACH 		foreign key (NACH) references FLUGHAFEN(FHC), constraint FLUG_VON_NN 		check (VON is not null), constraint FLUG_NACH_NN 		check (NACH is not null), constraint FLUG_AB_NN 		check (AB is not null), constraint FLUG_AN_NN 		check (AN is not null), constraint FLUG_AB_CHK 		check (AB between 0 and 2400), constraint FLUG_AN_CHK 		check (AN between 0 and 2400), constraint FLUG_VONNACH_CHK 		check (VON != NACH) ) HORIZONTAL (FLC('KK','MM'))",
                    "create table PASSAGIER ( PNR		integer, NAME		varchar(40), VORNAME		varchar(40),		 LAND		varchar(3), GEBORT      varchar(50), GEBDAT      varchar(20), GEBDATINT   integer,        constraint PASSAGIER_PS 		primary key (PNR), constraint PASSAGIER_NAME_NN         check (NAME is not null) ) VERTICAL ((PNR,NAME, VORNAME), (LAND, GEBORT), (GEBDAT, GEBDATINT))",
                    "create table BUCHUNG ( BNR             integer, PNR		integer, FLC		varchar(2), FLNR		integer,		 VON		varchar(3), NACH		varchar(3), TAG		varchar(20), MEILEN          integer, PREIS           integer, TAGINT		integer, constraint BUCHUNG_PS 		primary key (BNR), constraint BUCHUNG_FS_PNR 		foreign key (PNR) references PASSAGIER(PNR), constraint BUCHUNG_FS_FLC 		foreign key (FLC) references FLUGLINIE(FLC), constraint BUCHUNG_FS_VON 		foreign key (VON) references FLUGHAFEN(FHC), constraint BUCHUNG_FS_NACH 		foreign key (NACH) references FLUGHAFEN(FHC), constraint BUCHUNG_NACH_NN 		check (NACH is not null), constraint BUCHUNG_MEILEN_NN 		check (MEILEN is not null), constraint BUCHUNG_PREIS_NN 		check (PREIS is not null), constraint BUCHUNG_MEILEN_CHK                 check (MEILEN >= 0), constraint BUCHUNG_PREIS_CHK                 check (PREIS > 0) ) HORIZONTAL (PNR(35,25000))");
            for (String stmt : stmts) {
                assertEquals(0, executeUpdate(fedConnectionTest, stmt));
            }
        } catch (FedException ex) {
            throw ex;
        }
    }

    @Test
    public void testGroupBy() throws FedException, SQLException {
        testPARSELS1TGHAV_1();
        testPARSELS1TGHAV_2();
        testPARSELS1TGHAV_3();
        testPARSELS1TGP_1();
        testPARSELS1TGP_2();
        testPARSELS1TGP_3();
        testPARSELS1TWGP_1();
        testPARSELS1TWGP_2();
        testPARSELS1TWGP_3();
        testPARSELS1TWGP_4();
        testPARSELS1TWGP_5();
        testPARSELS1TWGP_6();
        testPARSEL1TGHAVLarge_1();
        testPARSEL1TGHAVLarge_2();
        testPARSEL1TGHAVLarge_3();
        testPARSEL1TGHAVSmall_1();
        testPARSEL1TGHAVSmall_2();
        testPARSEL1TGPLarge_1();
        testPARSEL1TGPLarge_2();
        testPARSEL1TGPLarge_3();
        testPARSEL1TGPLarge_4();
        testPARSEL1TGPSmall_1();
        testPARSEL1TGPSmall_2();
        testPARSEL1TGPSmall_3();
        testPARSEL1TWGPLarge_1();
        testPARSEL1TWGPLarge_2();
        testPARSEL1TWGPLarge_3();
        testPARSEL1TWGPLarge_4();
        testPARSEL1TWGPSmall_1();
        testPARSEL1TWGPSmall_2();
        testPARSEL1TWGPSmall_3();
        testPARSEL1TWGPSmall_4();
        testPARSELJoinWGP_1();
        testPARSELJoinWGP_2();
        testPARSELJoinWGP_3();
        testPARSELJoinWGP_4();
    }

    public void testPARSELS1TGHAV_1() throws FedException, SQLException {

        GroupByResults results = testResultsForFiles.get(1).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE GROUP BY FLUGLINIE.LAND");
        testAll(results, fs);
    }

    public void testPARSELS1TGHAV_2() throws FedException {

        GroupByResults results = testResultsForFiles.get(1).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT BUCHUNG.NACH, SUM(BUCHUNG.MEILEN) FROM BUCHUNG"
                + " GROUP BY BUCHUNG.NACH HAVING (COUNT (*) = 4)");
        testAll(results, fs);
    }

    public void testPARSELS1TGHAV_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(1).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT BUCHUNG.VON, SUM(BUCHUNG.PREIS) FROM BUCHUNG WHERE ((BUCHUNG.BNR > 20))"
                + " GROUP BY BUCHUNG.VON HAVING (COUNT (*) > 10)");
        testAll(results, fs);
    }

    public void testPARSELS1TGP_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(2).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUG.FLC, COUNT(*) FROM FLUG GROUP BY FLUG.FLC");
        testAll(results, fs);
    }

    public void testPARSELS1TGP_2() throws FedException {
        GroupByResults results = testResultsForFiles.get(2).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUG.FLC, SUM(FLUG.FNR) FROM FLUG GROUP BY FLUG.FLC");
        testAll(results, fs);
    }

    public void testPARSELS1TGP_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(2).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUG.FLC, COUNT(*) FROM FLUG GROUP BY FLUG.FLC");
        testAll(results, fs);
    }

    public void testPARSELS1TWGP_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(3).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE GROUP BY FLUGLINIE.LAND");
        testAll(results, fs);
    }

    public void testPARSELS1TWGP_2() throws FedException {

        GroupByResults results = testResultsForFiles.get(3).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE"
                + " WHERE ((FLUGLINIE.ALLIANZ = 'Star')) GROUP BY FLUGLINIE.LAND");
        testAll(results, fs);
    }

    public void testPARSELS1TWGP_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(3).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM BUCHUNG"
                + " WHERE ((BUCHUNG.MEILEN > 3000)) GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }

    public void testPARSELS1TWGP_4() throws FedException {
        GroupByResults results = testResultsForFiles.get(3).get(4);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM BUCHUNG"
                + " WHERE ((BUCHUNG.MEILEN > 3000) AND (BUCHUNG.MEILEN < 4000))"
                + " GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }

    public void testPARSELS1TWGP_5() throws FedException {
        GroupByResults results = testResultsForFiles.get(3).get(5);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM BUCHUNG"
                + " WHERE ((BUCHUNG.PNR = 1) OR (BUCHUNG.PNR = 9)) GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }

    public void testPARSELS1TWGP_6() throws FedException {
        GroupByResults results = testResultsForFiles.get(3).get(6);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT BUCHUNG.PNR, SUM(BUCHUNG.MEILEN) FROM BUCHUNG"
                + " WHERE ((BUCHUNG.MEILEN < 0)) GROUP BY BUCHUNG.PNR");
        testAll(results, fs);
    }

    public void testPARSEL1TGHAVLarge_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(4).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.NACH, COUNT(*) FROM BUCHUNG GROUP BY BUCHUNG.NACH HAVING (COUNT (*) = 4)");
        testAll(results, fs);
    }

    public void testPARSEL1TGHAVLarge_2() throws FedException {
        GroupByResults results = testResultsForFiles.get(4).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.VON, COUNT(*) FROM BUCHUNG WHERE ((BUCHUNG.BNR > 20)) GROUP BY BUCHUNG.VON HAVING "
                + "(COUNT (*) > 10)");
        testAll(results, fs);
    }

    public void testPARSEL1TGHAVLarge_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(4).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT PASSAGIER.LAND, COUNT(*) FROM PASSAGIER GROUP BY PASSAGIER.LAND HAVING (COUNT (*) > 24)");
        testAll(results, fs);
    }

    public void testPARSEL1TGHAVSmall_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(5).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.NACH, SUM(BUCHUNG.MEILEN) FROM BUCHUNG GROUP BY BUCHUNG.NACH HAVING (COUNT (*) = 4)");
        testAll(results, fs);
    }

    public void testPARSEL1TGHAVSmall_2() throws FedException {
        GroupByResults results = testResultsForFiles.get(5).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.VON, SUM(BUCHUNG.PREIS) FROM BUCHUNG WHERE ((BUCHUNG.PNR < 1000)) GROUP BY BUCHUNG"
                + ".VON HAVING (COUNT (*) > 6)");
        testAll(results, fs);
    }

    /**
     * @Test // Erster Fall des Docs ausgelassen da nur COUNT(*) public void
     * testPARSEL1TGPLarge_1() throws FedException{ GroupByResults results =
     * testResultsForFiles.get(6).get(1); FedResultSet fs = getResultSet("SELECT
     * COUNT(*) FROM BUCHUNG"); testAll(results, fs); }
     */
    public void testPARSEL1TGPLarge_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(6).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT BUCHUNG.FLC, COUNT(*) FROM BUCHUNG GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }

    public void testPARSEL1TGPLarge_2() throws FedException {
        GroupByResults results = testResultsForFiles.get(6).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT BUCHUNG.FLC, SUM(BUCHUNG.MEILEN) FROM BUCHUNG GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }

    public void testPARSEL1TGPLarge_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(6).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT PASSAGIER.LAND, COUNT(*) FROM PASSAGIER GROUP BY PASSAGIER.LAND");
        testAll(results, fs);
    }

    public void testPARSEL1TGPLarge_4() throws FedException {
        GroupByResults results = testResultsForFiles.get(6).get(4);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT PASSAGIER.GEBORT, COUNT(*) FROM PASSAGIER GROUP BY PASSAGIER.GEBORT");
        testAll(results, fs);
    }

    /**
     * @Test //Erster Fall des Docs ausgelassen da nur COUNT(*) public void
     * testPARSEL1TGPSmall_1() throws FedException{ GroupByResults results =
     * testResultsForFiles.get(7).get(1); FedResultSet fs = getResultSet("SELECT
     * COUNT(*) FROM FLUGHAFEN"); testAll(results, fs); }
     */
    public void testPARSEL1TGPSmall_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(7).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUG.FLC, COUNT(*) FROM FLUG GROUP BY FLUG.FLC");
        testAll(results, fs);
    }

    public void testPARSEL1TGPSmall_2() throws FedException {
        GroupByResults results = testResultsForFiles.get(7).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUG.FLC, SUM(FLUG.FNR) FROM FLUG GROUP BY FLUG.FLC");
        testAll(results, fs);
    }

    public void testPARSEL1TGPSmall_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(7).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUG.FLC, MAX(FLUG.FNR) FROM FLUG GROUP BY FLUG.FLC");
        testAll(results, fs);
    }

    public void testPARSEL1TWGPLarge_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(8).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest, "SELECT FLUGLINIE.ALLIANZ, COUNT(*) FROM FLUGLINIE GROUP BY FLUGLINIE.ALLIANZ");
        testAll(results, fs);
    }

    public void testPARSEL1TWGPLarge_2() throws FedException {
        GroupByResults results = testResultsForFiles.get(8).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE WHERE ((FLUGLINIE.ALLIANZ = 'Star')) GROUP BY "
                + "FLUGLINIE.LAND");
        testAll(results, fs);
    }

    public void testPARSEL1TWGPLarge_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(8).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.NACH, COUNT(*) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN > 4100) AND (BUCHUNG.BNR > 100950)"
                + ") GROUP BY BUCHUNG.NACH");
        testAll(results, fs);
    }

    public void testPARSEL1TWGPLarge_4() throws FedException {
        GroupByResults results = testResultsForFiles.get(8).get(4);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.VON, COUNT(*) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN > 16000) OR (BUCHUNG.BNR < 10)) "
                + "GROUP BY BUCHUNG.VON");
        testAll(results, fs);
    }

    public void testPARSEL1TWGPSmall_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(9).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT FLUGLINIE.LAND, COUNT(*) FROM FLUGLINIE WHERE ((FLUGLINIE.ALLIANZ = 'Star')) GROUP BY FLUGLINIE.LAND");
        testAll(results, fs);
    }

    public void testPARSEL1TWGPSmall_2() throws FedException {
        GroupByResults results = testResultsForFiles.get(9).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN > 3000)) GROUP BY BUCHUNG"
                + ".FLC");
        testAll(results, fs);
    }

    public void testPARSEL1TWGPSmall_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(9).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.FLC, MAX(BUCHUNG.PREIS) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN > 3000)) GROUP BY BUCHUNG"
                + ".FLC");
        testAll(results, fs);
    }

    public void testPARSEL1TWGPSmall_4() throws FedException {
        GroupByResults results = testResultsForFiles.get(9).get(4);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.PNR, SUM(BUCHUNG.MEILEN) FROM BUCHUNG WHERE ((BUCHUNG.MEILEN < 0)) GROUP BY BUCHUNG"
                + ".PNR");
        testAll(results, fs);
    }

    public void testPARSELJoinWGP_1() throws FedException {
        GroupByResults results = testResultsForFiles.get(10).get(1);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.FLC, MAX(BUCHUNG.PNR) FROM PASSAGIER, BUCHUNG WHERE (PASSAGIER.PNR = BUCHUNG.PNR) "
                + "GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }

    public void testPARSELJoinWGP_2() throws FedException {
        GroupByResults results = testResultsForFiles.get(10).get(2);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.FLC, COUNT(*) FROM PASSAGIER, BUCHUNG WHERE (PASSAGIER.PNR = BUCHUNG.PNR) AND ("
                + "(BUCHUNG.VON = 'FRA') AND (BUCHUNG.NACH = 'CDG')) GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }

    public void testPARSELJoinWGP_3() throws FedException {
        GroupByResults results = testResultsForFiles.get(10).get(3);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.FLC, SUM(BUCHUNG.PREIS) FROM PASSAGIER, BUCHUNG WHERE (PASSAGIER.PNR = BUCHUNG.PNR) "
                + "AND ((BUCHUNG.BNR = 13020) OR (BUCHUNG.BNR = 13201)) GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }

    public void testPARSELJoinWGP_4() throws FedException {
        GroupByResults results = testResultsForFiles.get(10).get(4);
        FedResultSet fs = getResultSet(fedConnectionTest,
                "SELECT BUCHUNG.FLC, COUNT(*) FROM PASSAGIER, BUCHUNG WHERE (PASSAGIER.PNR = BUCHUNG.PNR) AND ("
                + "(PASSAGIER.GEBDATINT > 14000)) GROUP BY BUCHUNG.FLC");
        testAll(results, fs);
    }
}
