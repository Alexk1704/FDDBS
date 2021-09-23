package junit.fdbs.sql.fedresultset;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedStatement;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.util.DatabaseCursorChecker;
import fdbs.util.logger.Logger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import junit.fdbs.sql.fedresultset.select.FlugRow;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class FedResultSetTest {

    protected static FedConnection fedConnection;
    protected static MetadataManager metadataManager;

    public FedResultSetTest() {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        DatabaseCursorChecker.reset();
        fedConnection = new FedConnection("VDBSA04", "VDBSA04");
        metadataManager = MetadataManager.getInstance(fedConnection, true);

        try {
            metadataManager.deleteTables();
        } catch (FedException ex) {
        }

        try {
            metadataManager.checkAndRefreshTables();
        } catch (FedException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGHAFEN", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGHAFEN", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGHAFEN", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE PASSAGIER", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE PASSAGIER", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE PASSAGIER", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGLINIE", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGLINIE", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGLINIE", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUG", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUG", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUG", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE BUCHUNG", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE BUCHUNG", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE BUCHUNG", 3);
        } catch (FedException | SQLException ex) {
        }
    }

    @After
    public void tearDown() throws FedException {
        try {
            metadataManager.deleteTables();
        } catch (FedException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGHAFEN", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGHAFEN", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGHAFEN", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE PASSAGIER", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE PASSAGIER", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE PASSAGIER", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGLINIE", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGLINIE", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUGLINIE", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUG", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUG", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE FLUG", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE BUCHUNG", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE BUCHUNG", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("DROP TABLE BUCHUNG", 3);
        } catch (FedException | SQLException ex) {
        }

        Logger.logln("################################################");
        Logger.logln("Openend Statements: " + DatabaseCursorChecker.getOpenedStatements());
        Logger.logln("Closed Statements: " + DatabaseCursorChecker.getClosedStatements());
        Logger.logln("Openend ResultSets: " + DatabaseCursorChecker.getOpenedResultSet());
        Logger.logln("Closed ResultSets: " + DatabaseCursorChecker.getClosedResultSet());
        Logger.logln("################################################");
        fedConnection.close();
        Logger.infoln("Finished to test general fed result set.");
    }

    protected int executeUpdate(String updateStatement) throws FedException {
        int result = 0;
        try (FedStatement statement = fedConnection.getStatement()) {
            result = statement.executeUpdate(updateStatement);
        }

        return result;
    }

    protected FedStatement executeQuery(String queryStatement) throws FedException {
        FedStatement statement = fedConnection.getStatement();
        statement.executeQuery(queryStatement);
        return statement;
    }

    protected void executeStatement(String stmt, Integer db) throws FedException, SQLException {
        SQLExecuter sqlExecuter = new SQLExecuter(fedConnection);
        try (SQLExecuterTask task = new SQLExecuterTask()) {
            if (db == 0) {
                task.addSubTask(stmt, true, 1);
                task.addSubTask(stmt, true, 2);
                task.addSubTask(stmt, true, 3);
            } else if (db == 1) {
                task.addSubTask(stmt, true, 1);
            } else if (db == 2) {
                task.addSubTask(stmt, true, 2);
            } else if (db == 3) {
                task.addSubTask(stmt, true, 3);
            }

            sqlExecuter.executeTask(task);
        }
    }

    protected void insertFLUGHAFEN() throws FedException {
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('AKL', 'NZ ', 'Auckland', 'Auckland International')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ALC', 'E  ', 'Alicante', 'Alicante')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ALF', 'N  ', 'Alta', 'Flughafen Alta')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ANC', 'USA', 'Anchorage', 'Ted Stevens AIA')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ARN', 'S  ', 'Stockholm', 'Arlanda')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('BCN', 'E  ', 'Barcelona', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('BHX', 'GB ', 'Birmingham', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('BOS', 'USA', 'Boston', 'Edward Lawrence Logan')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('BRE', 'D  ', 'Bremen', 'City Airport Bremen')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('BRS', 'GB ', 'Bristol', 'International')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('CDG', 'F  ', 'Paris', 'Charles de Gaulle')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('CGK', 'ID ', 'Jakarta', 'Sukarno-Hatta Airport')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('CGN', 'D  ', 'Koeln', 'Konrad-Adenauer')"));

        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('CPH', 'DK ', 'Kopenhagen', 'Kastrup')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('CPT', 'RSA', 'Kapstadt', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('CRT', 'TUN', 'Chartage', 'Tunesien')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('DJE', 'TUN', 'Djerba', 'Aeroport de Djerba')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('DRS', 'D  ', 'Dresden', 'Dresden Airport')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('DUS', 'D  ', 'Duesseldorf', 'Duesseldorf International')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('EDI', 'GB ', 'Edinburgh', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ERF', 'D  ', 'Erfurt', 'Flughafen Erfurt(ERF)')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('FCO', 'I  ', 'Rom', 'Fiumicino')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('FDH', 'D  ', 'Friedrichshafen', 'Bodensee-Airport')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('FLR', 'I  ', 'Florenz', 'Peretola')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('FRA', 'D  ', 'Frankfurt', 'Rhein-Main')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('GOT', 'S  ', 'Goeteborg', 'Landvetter')"));

        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('GRZ', 'A  ', 'Graz', 'Flughafen Graz')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('GVA', 'CH ', 'Genf', 'Flughafen Genf')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('HAJ', 'D  ', 'Hannover', 'Langenhagen')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('HHN', 'D  ', 'Hahn', 'Flughafen Hahn')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('HKG', 'CHN', 'HongKong', 'HongKong International')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('HRG', 'EG ', 'Hurghada', 'International')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('IAD', 'USA', 'Washington', 'Dulles')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ISA', 'AUS', 'Mount Isa', 'Queensland')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('IST', 'TR ', 'Istanbul', 'Atat�rk')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('JFK', 'USA', 'New York', 'John F. Kennedy')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('KIX', 'J  ', 'Osaka', 'Kansai')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('KLU', 'D  ', 'Klagenfurt', 'Klagenfurter Flughafen')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('KSF', 'D  ', 'Kassel', 'Calden')"));

        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('LAX', 'USA', 'Los Angeles', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('LGW', 'GB ', 'London', 'Gatwick')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('LHR', 'GB ', 'London', 'Heathrow')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('LIM', 'PE ', 'Lima', 'Jorge Ch�vez')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('LPA', 'E  ', 'Las Palmas', 'Las Palmas Airport')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('LYS', 'F  ', 'Lyon', 'Saint-Exup�ry')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('MAN', 'GB ', 'Manchester', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('MCO', 'USA', 'Orlando', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('MIR', 'TUN', 'Monastir', 'A�roport Monastir')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('MPL', 'F  ', 'Montpellier', 'Montpellier Airport')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('MRS', 'F  ', 'Marseille', 'Marseille Provence')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('MUC', 'D  ', 'Munich', 'Franz-Josef Strauss')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('MVR', 'CMR', 'Maroua', 'Maroua Salek Airport')"));

        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('NAP', 'I  ', 'Neapel', 'Capodichino')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('NCE', 'D  ', 'Nice', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('NRT', 'J  ', 'Tokio', 'Narita')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('NUE', 'D  ', 'Nuernberg', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ORD', 'USA', 'Chicago', 'OHare')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ORY', 'F  ', 'Paris', 'Orly')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('OSL', 'N  ', 'Oslo', 'Oslo Gardermoen')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('PAD', 'D  ', 'Paderborn', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('PEK', 'CHN', 'Peking', 'Beijing Shoudu Guoji Jichang')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('PMI', 'E  ', 'Palma', 'Aeropuerto de Son Sant Joan')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('SEA', 'USA', 'Seattle', 'Seattle-Tacoma')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('SFO', 'USA', 'San Francisco', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('SSH', 'ET ', 'Sharm El Sheikh', 'Sharm El Sheikh Nat. Airport')"));

        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('STN', 'GB ', 'London', 'Stanstead')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('SVO', 'RUS', 'Moskau', 'Scheremetjewo')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('SZG', 'A  ', 'Salzburg', 'Salzburg Airport')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('TFS', 'E  ', 'Granadilla', 'Teneriffa/S�d')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('TLS', 'F  ', 'Toulouse', 'Blagnac')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('TRD', 'N  ', 'Trondheim', 'Vaernes')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('TRU', 'PE ', 'Trujillo', '')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('TUN', 'TUN', 'Aeroport de Tunis', 'Tunis')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('TXL', 'D  ', 'Berlin', 'Tegel')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('VCE', 'I  ', 'Venedig', 'Marco Polo')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('VIE', 'A  ', 'Wien', 'Schwechat')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('VLC', 'E  ', 'Valencia', 'Manises')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('YDE', 'CMR', 'Yaounde', null)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('YUL', 'CDN', 'Montreal', 'Pierre Elliot Trudeau')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('YVR', 'CDN', 'Vancouver', 'Vancouver International')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('YYC', 'CDN', 'Calgary', 'Municipal Airport')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('YYZ', 'CDN', 'Toronto', 'Lester Pearson')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ZAZ', 'E  ', 'Zaragoza', null)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGHAFEN VALUES ('ZRH', 'CH ', 'Zuerich', 'Kloten')"));
    }

    protected void inserFLUGLINIE() throws FedException {
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('AB', 'D  ', null, 'Air Berlin', null)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('AC', 'CDN', null, 'Air Canada', 'Star')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('AF', 'F  ', null, 'Air France', 'SkyTeam')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('BA', 'GB ', null, 'British Airways', 'OneWorld')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('DB', 'D  ', null, 'Database Airlines', null)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('DI', 'D  ', null, 'Deutsche BA', null)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('DL', 'USA', null, 'Delta Airlines', 'SkyTeam')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('JL', 'J  ', null, 'Japan Airlines', 'OneWorld')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('LH', 'D  ', null, 'Lufthansa', 'Star')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('NH', 'J  ', null, 'All Nippon Airways', 'Star')"));
        assertEquals(1, executeUpdate("INSERT INTO FLUGLINIE VALUES ('UA', 'USA', null, 'United Airlines', 'Star')"));
    }

    protected void insertFLUG() throws FedException {
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (91, 'AC', 10, 'YYZ', 'FRA', 1815, 740)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (52, 'AC', 11, 'YUL', 'YYZ', 1500, 1700)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (18, 'AF', 9, 'TXL', 'CDG', 915, 1040)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (74, 'AF', 33, 'FRA', 'CDG', 900, 1010)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (84, 'AF', 34, 'FRA', 'CDG', 1200, 1210)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (1, 'AF', 35, 'CDG', 'FRA', 1400, 1500)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (92, 'AF', 45, 'CDG', 'NRT', 1220, 730)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (2, 'BA', 84, 'ORD', 'SFO', 1530, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (66, 'BA', 86, 'FRA', 'LHR', 910, 1050)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (3, 'BA', 87, 'LHR', 'FRA', 1310, 1450)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (4, 'BA', 88, 'LHR', 'BHX', 1010, 1050)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (5, 'BA', 90, 'LGW', 'BHX', 1020, 1150)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (36, 'DB', 2, 'HHN', 'FRA', 1120, 1230)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (85, 'DB', 3, 'FRA', 'HHN', 1530, 1640)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (19, 'DB', 6, 'ERF', 'FRA', 915, 945)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (75, 'DB', 7, 'FRA', 'ERF', 1000, 1030)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (20, 'DB', 8, 'MIR', 'FRA', 1000, 1230)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (21, 'DB', 9, 'FRA', 'MIR', 1500, 1730)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (67, 'DB', 10, 'DJE', 'FRA', 1000, 1230)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (6, 'DB', 11, 'FRA', 'DJE', 1500, 1730)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (76, 'DB', 14, 'LYS', 'FRA', 1845, 1955)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (37, 'DB', 15, 'FRA', 'LYS', 115, 230)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (38, 'DB', 16, 'PMI', 'FRA', 455, 650)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (68, 'DB', 17, 'FRA', 'PMI', 915, 1150)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (93, 'DB', 18, 'DRS', 'FRA', 600, 700)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (77, 'DB', 19, 'FRA', 'DRS', 1750, 1850)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (39, 'DB', 20, 'ALF', 'FRA', 630, 700)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (78, 'DB', 21, 'FRA', 'ALF', 930, 1050)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (86, 'DB', 22, 'PEK', 'FRA', 0, 800)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (53, 'DB', 23, 'FRA', 'PEK', 1500, 2300)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (40, 'DB', 24, 'KLU', 'FRA', 1050, 1250)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (22, 'DB', 25, 'FRA', 'KLU', 1450, 1650)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (23, 'DB', 28, 'TUN', 'FRA', 1000, 1230)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (24, 'DB', 29, 'FRA', 'TUN', 1500, 1730)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (94, 'DB', 32, 'MPL', 'FRA', 820, 1050)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (69, 'DB', 33, 'FRA', 'MPL', 1210, 1440)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (25, 'DB', 36, 'HAJ', 'FRA', 800, 900)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (26, 'DB', 37, 'FRA', 'HAJ', 1200, 1300)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (95, 'DB', 40, 'YYC', 'FRA', 815, 1715)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (27, 'DB', 41, 'FRA', 'YYC', 1830, 330)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (28, 'DB', 42, 'SSH', 'FRA', 500, 900)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (29, 'DB', 43, 'FRA', 'SSH', 1100, 1400)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (7, 'DB', 46, 'LIM', 'FRA', 2055, 1830)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (79, 'DB', 47, 'FRA', 'LIM', 2015, 820)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (8, 'DB', 48, 'DUS', 'FRA', 915, 1000)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (41, 'DB', 49, 'FRA', 'DUS', 1115, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (96, 'DB', 54, 'SVO', 'FRA', 1620, 1745)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (80, 'DB', 55, 'FRA', 'SVO', 1845, 2010)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (87, 'DB', 56, 'BRE', 'FRA', 615, 700)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (9, 'DB', 57, 'FRA', 'BRE', 800, 845)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (54, 'DB', 58, 'LPA', 'FRA', 100, 550)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (10, 'DB', 59, 'FRA', 'LPA', 1700, 2150)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (42, 'DB', 60, 'ISA', 'FRA', 415, 32)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (43, 'DB', 61, 'FRA', 'ISA', 1750, 909)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (55, 'DB', 62, 'HKG', 'FRA', 1000, 1700)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (56, 'DB', 63, 'FRA', 'HKG', 1900, 800)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (11, 'DB', 72, 'YDE', 'FRA', 2315, 725)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (57, 'DB', 73, 'FRA', 'YDE', 1740, 2340)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (58, 'DB', 74, 'CGK', 'FRA', 2300, 1000)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (12, 'DB', 75, 'FRA', 'CGK', 1700, 1300)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (70, 'DB', 76, 'MVR', 'FRA', 1000, 1700)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (59, 'DB', 77, 'FRA', 'MVR', 1800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (44, 'DB', 78, 'TLS', 'FRA', 830, 1000)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (71, 'DB', 79, 'FRA', 'TLS', 1230, 1400)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (72, 'DB', 80, 'SZG', 'FRA', 800, 1000)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (45, 'DB', 81, 'FRA', 'SZG', 1200, 1400)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (60, 'DB', 82, 'CRT', 'FRA', 1000, 1230)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (61, 'DB', 83, 'FRA', 'CRT', 1500, 1730)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (46, 'DB', 90, 'TRU', 'FRA', 2200, 1020)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (30, 'DB', 91, 'FRA', 'TRU', 1430, 2200)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (13, 'DL', 7, 'ORD', 'SFO', 1140, 1530)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (14, 'DL', 9, 'LAX', 'NRT', 2220, 1000)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (97, 'DL', 33, 'SFO', 'LAX', 900, 1005)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (98, 'JL', 12, 'TXL', 'KIX', 1355, 820)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (31, 'LH', 5, 'FRA', 'TXL', 730, 830)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (62, 'LH', 6, 'TXL', 'FRA', 930, 1030)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (88, 'LH', 7, 'FRA', 'TXL', 1130, 1230)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (63, 'LH', 8, 'TXL', 'FRA', 1230, 1330)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (47, 'LH', 9, 'FRA', 'TXL', 1430, 1530)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (48, 'LH', 10, 'TXL', 'FRA', 1630, 1730)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (15, 'LH', 20, 'TXL', 'CDG', 900, 1140)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (32, 'LH', 24, 'TXL', 'FRA', 2130, 2310)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (89, 'LH', 32, 'JFK', 'LAX', 1400, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (16, 'LH', 34, 'FRA', 'SFO', 1015, 1245)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (49, 'LH', 36, 'SFO', 'LAX', 1700, 1815)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (90, 'LH', 40, 'FRA', 'LHR', 700, 815)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (99, 'LH', 41, 'LHR', 'FRA', 1025, 1145)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (50, 'LH', 42, 'FRA', 'LHR', 1300, 1415)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (81, 'LH', 43, 'LHR', 'FRA', 1515, 1640)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (64, 'LH', 44, 'FRA', 'LHR', 1600, 1715)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (17, 'LH', 45, 'LHR', 'FRA', 1830, 2000)"));

        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (33, 'LH', 46, 'FRA', 'LHR', 2130, 2245)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (51, 'LH', 47, 'LHR', 'FRA', 700, 820)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (34, 'LH', 50, 'FRA', 'CDG', 710, 810)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (100, 'LH', 51, 'CDG', 'FRA', 930, 1030)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (82, 'LH', 52, 'FRA', 'CDG', 1210, 1310)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (35, 'LH', 53, 'CDG', 'FRA', 1500, 1620)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (73, 'LH', 54, 'FRA', 'CDG', 1740, 1850)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (83, 'LH', 55, 'CDG', 'FRA', 2000, 2120)"));
        assertEquals(1, executeUpdate("INSERT INTO FLUG VALUES (65, 'LH', 60, 'FRA', 'BHX', 1005, 1130)"));
    }

    protected void inserBUCHUNG() throws FedException {
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (71, 1, 'LH', 32, 'FRA', 'LAX', '03-SEP-1997', 5800, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (72, 1, 'LH', 34, 'FRA', 'SFO', '09-NOV-1997', 5700, 2800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (73, 2, 'DL', 9, 'FRA', 'LAX', '09-NOV-1997', 5800, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (74, 3, 'AF', 33, 'FRA', 'CDG', '10-OCT-1997', 300, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (75, 3, 'AF', 45, 'CDG', 'NRT', '10-OCT-1997', 6200, 8000)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (77, 5, 'AF', 9, 'TXL', 'CDG', '09-SEP-1997', 500, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (78, 5, 'AF', 9, 'TXL', 'CDG', '29-SEP-1997', 500, 800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (79, 5, 'AF', 9, 'TXL', 'CDG', '12-DEC-1997', 500, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (76, 5, 'LH', 32, 'FRA', 'JFK', '12-AUG-1997', 3900, 2000)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (80, 6, 'DL', 9, 'JFK', 'NRT', '13-NOV-1997', 6100, 4000)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (82, 7, 'AF', 33, 'FRA', 'CDG', '12-OCT-1997', 300, 1900)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (81, 7, 'DL', 9, 'JFK', 'NRT', '15-FEB-1997', 6100, 4000)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (85, 9, 'DL', 7, 'FRA', 'SFO', '17-AUG-1997', 5700, 4000)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (86, 9, 'DL', 9, 'FRA', 'JFK', '20-DEC-1997', 3900, 2000)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (87, 9, 'DL', 33, 'SFO', 'LAX', '09-OCT-1997', 500, 600)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (88, 9, 'DL', 33, 'SFO', 'LAX', '23-NOV-1997', 500, 800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (84, 9, 'JL', 12, 'FRA', 'KIX', '18-NOV-1997', 7200, 5400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (83, 9, 'LH', 34, 'FRA', 'SFO', '29-JUL-1997', 5700, 800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (89, 11, 'LH', 5, 'FRA', 'TXL', '11-APR-1998', 500, 700)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (90, 12, 'AF', 45, 'CDG', 'NRT', '10-JAN-1998', 6200, 2500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (91, 16, 'LH', 40, 'FRA', 'LHR', '18-MAY-1998', 500, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (24, 21, 'DB', 2, 'HHN', 'FRA', '12-AUG-2010', 40, 60)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (26, 21, 'DB', 2, 'HHN', 'FRA', '12-AUG-2012', 40, 60)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (25, 21, 'DB', 3, 'FRA', 'HHN', '12-SEP-2010', 40, 60)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (29, 21, 'DB', 3, 'FRA', 'HHN', '12-OCT-2012', 40, 60)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (28, 21, 'LH', 51, 'CDG', 'FRA', '12-OCT-2012', 300, 250)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (27, 21, 'LH', 54, 'FRA', 'CDG', '12-AUG-2012', 300, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (30, 22, 'DB', 42, 'SSH', 'FRA', '01-MAY-2010', 2000, 300)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (32, 22, 'DB', 42, 'SSH', 'FRA', '01-MAY-2011', 2000, 300)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (31, 22, 'DB', 43, 'FRA', 'SSH', '03-MAY-2010', 2000, 300)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (35, 22, 'DB', 43, 'FRA', 'SSH', '08-MAY-2011', 2000, 300)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (33, 22, 'LH', 42, 'FRA', 'LHR', '01-MAY-2011', 400, 60)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (34, 22, 'LH', 47, 'LHR', 'FRA', '08-MAY-2011', 400, 60)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (121, 56, 'BA', 87, 'LHR', 'FRA', '24-MAR-2011', 1200, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (98, 56, 'DB', 24, 'KLU', 'FRA', '16-MAR-2010', 460, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (100, 56, 'DB', 24, 'KLU', 'FRA', '16-MAR-2011', 460, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (99, 56, 'DB', 25, 'FRA', 'KLU', '18-MAR-2010', 460, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (103, 56, 'DB', 25, 'FRA', 'KLU', '23-MAR-2011', 460, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (59, 56, 'DB', 36, 'HAJ', 'FRA', '04-FEB-2010', 400, 500)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (61, 56, 'DB', 36, 'HAJ', 'FRA', '04-FEB-2011', 400, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (60, 56, 'DB', 37, 'FRA', 'HAJ', '06-FEB-2010', 400, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (64, 56, 'DB', 37, 'FRA', 'HAJ', '11-FEB-2011', 400, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (118, 56, 'DB', 56, 'BRE', 'FRA', '19-MAR-2010', 450, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (119, 56, 'DB', 56, 'BRE', 'FRA', '17-MAR-2011', 450, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (117, 56, 'DB', 57, 'FRA', 'BRE', '17-MAR-2010', 450, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (122, 56, 'DB', 57, 'FRA', 'BRE', '25-MAR-2011', 450, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (102, 56, 'LH', 41, 'LHR', 'FRA', '23-MAR-2011', 460, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (62, 56, 'LH', 42, 'FRA', 'LHR', '04-FEB-2011', 500, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (101, 56, 'LH', 44, 'FRA', 'LHR', '16-MAR-2011', 660, 600)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (120, 56, 'LH', 46, 'FRA', 'LHR', '17-MAR-2011', 1200, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (63, 56, 'LH', 47, 'LHR', 'FRA', '11-FEB-2011', 500, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (65, 57, 'DB', 46, 'LIM', 'FRA', '04-DEC-2010', 10000, 1000)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (67, 57, 'DB', 46, 'LIM', 'FRA', '04-DEC-2011', 10000, 1000)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (66, 57, 'DB', 47, 'FRA', 'LIM', '06-DEC-2010', 10000, 1000)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (70, 57, 'DB', 47, 'FRA', 'LIM', '11-DEC-2011', 10000, 1000)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (68, 57, 'LH', 46, 'FRA', 'LHR', '04-DEC-2011', 500, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (69, 57, 'LH', 47, 'LHR', 'FRA', '11-DEC-2011', 500, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (48, 58, 'DB', 22, 'PEK', 'FRA', '12-APR-2010', 5800, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (50, 58, 'DB', 22, 'PEK', 'FRA', '12-APR-2011', 5800, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (49, 58, 'DB', 23, 'FRA', 'PEK', '14-APR-2010', 5800, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (53, 58, 'DB', 23, 'FRA', 'PEK', '19-APR-2010', 5800, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (51, 58, 'LH', 42, 'FRA', 'LHR', '12-APR-2011', 800, 80)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (52, 58, 'LH', 47, 'LHR', 'FRA', '19-APR-2010', 800, 80)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (92, 59, 'DB', 74, 'CGK', 'FRA', '10-NOV-2010', 10000, 550)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (94, 59, 'DB', 74, 'CGK', 'FRA', '10-NOV-2011', 2000, 150)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (93, 59, 'DB', 75, 'FRA', 'CGK', '12-NOV-2010', 10000, 550)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (97, 59, 'DB', 75, 'FRA', 'CGK', '17-NOV-2011', 4000, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (95, 59, 'LH', 42, 'FRA', 'LHR', '10-NOV-2011', 4000, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (96, 59, 'LH', 47, 'LHR', 'FRA', '17-NOV-2011', 4000, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (21, 60, 'AF', 34, 'FRA', 'CDG', '25-MAY-2012', 120, 480)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (18, 60, 'DB', 48, 'DUS', 'FRA', '25-MAY-2010', 100, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (20, 60, 'DB', 48, 'DUS', 'FRA', '25-MAY-2012', 100, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (19, 60, 'DB', 49, 'FRA', 'DUS', '25-JUN-2010', 105, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (23, 60, 'DB', 49, 'FRA', 'DUS', '26-JUL-2012', 105, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (22, 60, 'LH', 51, 'CDG', 'FRA', '25-JUL-2012', 480, 120)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (104, 62, 'DB', 32, 'MPL', 'FRA', '23-NOV-2010', 610, 1280)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (55, 62, 'DB', 32, 'MPL', 'FRA', '23-NOV-2011', 610, 1280)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (54, 62, 'DB', 33, 'FRA', 'MPL', '25-NOV-2010', 610, 1430)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (58, 62, 'DB', 33, 'FRA', 'MPL', '30-NOV-2011', 610, 450)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (56, 62, 'LH', 42, 'FRA', 'LHR', '24-NOV-2011', 910, 320)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (57, 62, 'LH', 47, 'LHR', 'FRA', '30-NOV-2011', 880, 240)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (114, 63, 'BA', 86, 'FRA', 'LHR', '04-NOV-2011', 2800, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (111, 63, 'DB', 20, 'ALF', 'FRA', '04-NOV-2010', 2800, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (113, 63, 'DB', 20, 'ALF', 'FRA', '04-NOV-2011', 2800, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (112, 63, 'DB', 21, 'FRA', 'ALF', '06-NOV-2010', 2800, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (116, 63, 'DB', 21, 'FRA', 'ALF', '12-OCT-2011', 2800, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (115, 63, 'LH', 47, 'LHR', 'FRA', '11-NOV-2011', 2800, 1800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (36, 64, 'DB', 40, 'YYC', 'FRA', '20-AUG-2010', 5000, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (38, 64, 'DB', 40, 'YYC', 'FRA', '20-AUG-2011', 5000, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (37, 64, 'DB', 41, 'FRA', 'YYC', '20-AUG-2010', 5000, 1200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (41, 64, 'DB', 41, 'FRA', 'YYC', '27-AUG-2011', 5000, 1200)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (39, 64, 'LH', 46, 'FRA', 'LHR', '20-AUG-2011', 700, 120)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (40, 64, 'LH', 47, 'LHR', 'FRA', '27-AUG-2011', 700, 120)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (108, 66, 'BA', 86, 'FRA', 'LHR', '04-AUG-2011', 700, 70)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (105, 66, 'DB', 58, 'LPA', 'FRA', '04-AUG-2010', 4000, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (107, 66, 'DB', 58, 'LPA', 'FRA', '04-AUG-2011', 4000, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (106, 66, 'DB', 59, 'FRA', 'LPA', '06-AUG-2010', 4000, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (110, 66, 'DB', 59, 'FRA', 'LPA', '11-AUG-2011', 4000, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (109, 66, 'LH', 47, 'LHR', 'FRA', '11-AUG-2011', 700, 70)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (42, 67, 'DB', 82, 'CRT', 'FRA', '08-FEB-2010', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (44, 67, 'DB', 82, 'CRT', 'FRA', '08-FEB-2012', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (43, 67, 'DB', 83, 'FRA', 'CRT', '08-MAR-2010', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (47, 67, 'DB', 83, 'FRA', 'CRT', '08-APR-2012', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (46, 67, 'LH', 51, 'CDG', 'FRA', '08-APR-2012', 800, 100)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (45, 67, 'LH', 54, 'FRA', 'CDG', '08-FEB-2012', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (141, 68, 'DB', 6, 'ERF', 'FRA', '11-APR-2010', 190, 90)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (143, 68, 'DB', 6, 'ERF', 'FRA', '11-APR-2011', 190, 90)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (142, 68, 'DB', 7, 'FRA', 'ERF', '13-APR-2010', 190, 90)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (146, 68, 'DB', 7, 'FRA', 'ERF', '19-APR-2011', 190, 90)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (144, 68, 'LH', 42, 'FRA', 'LHR', '11-APR-2011', 190, 90)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (145, 68, 'LH', 47, 'LHR', 'FRA', '18-APR-2011', 190, 90)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (147, 69, 'DB', 28, 'TUN', 'FRA', '14-MAR-2010', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (136, 69, 'DB', 28, 'TUN', 'FRA', '14-MAR-2012', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (135, 69, 'DB', 29, 'FRA', 'TUN', '14-APR-2010', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (139, 69, 'DB', 29, 'FRA', 'TUN', '14-MAY-2012', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (138, 69, 'LH', 51, 'CDG', 'FRA', '14-MAY-2012', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (137, 69, 'LH', 54, 'FRA', 'CDG', '14-MAR-2012', 800, 100)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (150, 70, 'AF', 34, 'FRA', 'CDG', '27-FEB-2012', 330, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (140, 70, 'DB', 80, 'SZG', 'FRA', '27-FEB-2010', 330, 300)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (149, 70, 'DB', 80, 'SZG', 'FRA', '27-FEB-2012', 330, 300)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (148, 70, 'DB', 81, 'FRA', 'SZG', '27-MAR-2010', 330, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (152, 70, 'DB', 81, 'FRA', 'SZG', '28-APR-2012', 330, 400)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (151, 70, 'LH', 51, 'CDG', 'FRA', '27-APR-2012', 330, 300)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (153, 71, 'DB', 10, 'DJE', 'FRA', '06-JAN-2010', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (155, 71, 'DB', 10, 'DJE', 'FRA', '06-JAN-2012', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (154, 71, 'DB', 11, 'FRA', 'DJE', '06-FEB-2010', 2800, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (158, 71, 'DB', 11, 'FRA', 'DJE', '06-MAR-2012', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (157, 71, 'LH', 51, 'CDG', 'FRA', '06-MAR-2012', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (156, 71, 'LH', 54, 'FRA', 'CDG', '06-JAN-2012', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (177, 72, 'DB', 72, 'YDE', 'FRA', '18-JUN-2010', 500, 900)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (179, 72, 'DB', 72, 'YDE', 'FRA', '18-JUN-2011', 500, 900)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (178, 72, 'DB', 73, 'FRA', 'YDE', '20-JUN-2010', 500, 900)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (182, 72, 'DB', 73, 'FRA', 'YDE', '25-JUN-2011', 500, 900)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (180, 72, 'LH', 42, 'FRA', 'LHR', '18-JUN-2011', 500, 900)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (181, 72, 'LH', 47, 'LHR', 'FRA', '25-JUN-2011', 500, 900)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (159, 73, 'DB', 8, 'MIR', 'FRA', '10-OCT-2010', 1570, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (161, 73, 'DB', 8, 'MIR', 'FRA', '10-OCT-2011', 1570, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (160, 73, 'DB', 9, 'FRA', 'MIR', '12-OCT-2010', 1570, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (164, 73, 'DB', 9, 'FRA', 'MIR', '17-OCT-2011', 1570, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (162, 73, 'LH', 44, 'FRA', 'LHR', '10-OCT-2011', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (163, 73, 'LH', 47, 'LHR', 'FRA', '17-OCT-2011', 800, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (183, 74, 'DB', 62, 'HKG', 'FRA', '25-APR-2010', 6000, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (2, 74, 'DB', 62, 'HKG', 'FRA', '25-APR-2011', 2000, 150)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (1, 74, 'DB', 63, 'FRA', 'HKG', '27-APR-2010', 6000, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (5, 74, 'DB', 63, 'FRA', 'HKG', '01-MAY-2011', 4000, 250)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (3, 74, 'LH', 46, 'FRA', 'LHR', '25-APR-2011', 3000, 200)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (4, 74, 'LH', 47, 'LHR', 'FRA', '01-MAY-2011', 2000, 150)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (168, 75, 'AF', 34, 'FRA', 'CDG', '27-JUN-2012', 354, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (165, 75, 'DB', 78, 'TLS', 'FRA', '27-JUN-2010', 750, 190)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (167, 75, 'DB', 78, 'TLS', 'FRA', '27-JUN-2012', 750, 190)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (166, 75, 'DB', 79, 'FRA', 'TLS', '27-JUN-2010', 750, 190)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (170, 75, 'DB', 79, 'FRA', 'TLS', '27-AUG-2012', 750, 190)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (169, 75, 'LH', 51, 'CDG', 'FRA', '27-AUG-2012', 354, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (171, 76, 'DB', 54, 'SVO', 'FRA', '25-OCT-2010', 1236, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (173, 76, 'DB', 54, 'SVO', 'FRA', '25-OCT-2012', 1236, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (172, 76, 'DB', 55, 'FRA', 'SVO', '25-NOV-2010', 1236, 100)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (176, 76, 'DB', 55, 'FRA', 'SVO', '25-DEC-2012', 1236, 100)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (174, 76, 'LH', 50, 'FRA', 'CDG', '26-OCT-2012', 360, 80)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (175, 76, 'LH', 51, 'CDG', 'FRA', '25-DEC-2012', 360, 80)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (14, 77, 'AF', 33, 'FRA', 'CDG', '04-JUN-2012', 700, 330)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (11, 77, 'DB', 18, 'DRS', 'FRA', '04-JUN-2010', 300, 120)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (13, 77, 'DB', 18, 'DRS', 'FRA', '04-JUN-2012', 300, 120)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (12, 77, 'DB', 19, 'FRA', 'DRS', '04-JUL-2010', 300, 120)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (16, 77, 'DB', 19, 'FRA', 'DRS', '04-AUG-2012', 300, 120)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (15, 77, 'LH', 51, 'CDG', 'FRA', '04-AUG-2012', 700, 330)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (17, 78, 'DB', 14, 'LYS', 'FRA', '22-DEC-2010', 343, 70)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (7, 78, 'DB', 14, 'LYS', 'FRA', '22-DEC-2012', 343, 70)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (6, 78, 'DB', 15, 'FRA', 'LYS', '22-JAN-2011', 343, 70)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (10, 78, 'DB', 15, 'FRA', 'LYS', '23-FEB-2013', 343, 70)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (8, 78, 'LH', 50, 'FRA', 'CDG', '23-DEC-2012', 350, 75)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (9, 78, 'LH', 51, 'CDG', 'FRA', '22-FEB-2013', 350, 75)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (126, 79, 'AF', 33, 'FRA', 'CDG', '17-JUL-2012', 500, 120)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (123, 79, 'DB', 16, 'PMI', 'FRA', '17-JUL-2010', 1500, 230)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (125, 79, 'DB', 16, 'PMI', 'FRA', '17-JUL-2012', 1500, 230)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (124, 79, 'DB', 17, 'FRA', 'PMI', '17-AUG-2010', 1500, 260)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (128, 79, 'DB', 17, 'FRA', 'PMI', '18-SEP-2012', 1500, 260)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (127, 79, 'LH', 51, 'CDG', 'FRA', '17-SEP-2012', 500, 130)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (129, 80, 'DB', 60, 'ISA', 'FRA', '15-SEP-2010', 14427, 851)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (131, 80, 'DB', 60, 'ISA', 'FRA', '15-SEP-2012', 14727, 821)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (130, 80, 'DB', 61, 'FRA', 'ISA', '15-OCT-2010', 14429, 834)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (134, 80, 'DB', 61, 'FRA', 'ISA', '15-NOV-2012', 14469, 895)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (133, 80, 'LH', 51, 'CDG', 'FRA', '15-NOV-2012', 421, 216)"));

        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (132, 80, 'LH', 54, 'FRA', 'CDG', '16-SEP-2012', 450, 243)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (184, 81, 'DB', 90, 'TRU', 'FRA', '16-FEB-2010', 5000, 800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (186, 81, 'DB', 90, 'TRU', 'FRA', '16-FEB-2012', 5000, 800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (185, 81, 'DB', 91, 'FRA', 'TRU', '16-MAR-2010', 5000, 800)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (188, 81, 'LH', 51, 'CDG', 'FRA', '16-APR-2012', 5000, 500)"));
        assertEquals(1, executeUpdate("INSERT INTO BUCHUNG VALUES (187, 81, 'LH', 54, 'FRA', 'CDG', '17-FEB-2012', 5000, 500)"));
    }

    protected ArrayList<FlugRow> getFlugRows() {
        ArrayList<FlugRow> rows = new ArrayList<FlugRow>() {
            {
                add(new FlugRow(91, "AC", 10, "YYZ", "FRA", 1815, 740));
                add(new FlugRow(52, "AC", 11, "YUL", "YYZ", 1500, 1700));
                add(new FlugRow(18, "AF", 9, "TXL", "CDG", 915, 1040));
                add(new FlugRow(74, "AF", 33, "FRA", "CDG", 900, 1010));
                add(new FlugRow(84, "AF", 34, "FRA", "CDG", 1200, 1210));
                add(new FlugRow(1, "AF", 35, "CDG", "FRA", 1400, 1500));
                add(new FlugRow(92, "AF", 45, "CDG", "NRT", 1220, 730));
                add(new FlugRow(2, "BA", 84, "ORD", "SFO", 1530, 1800));
                add(new FlugRow(66, "BA", 86, "FRA", "LHR", 910, 1050));
                add(new FlugRow(3, "BA", 87, "LHR", "FRA", 1310, 1450));
                add(new FlugRow(4, "BA", 88, "LHR", "BHX", 1010, 1050));
                add(new FlugRow(5, "BA", 90, "LGW", "BHX", 1020, 1150));
                add(new FlugRow(36, "DB", 2, "HHN", "FRA", 1120, 1230));

                add(new FlugRow(85, "DB", 3, "FRA", "HHN", 1530, 1640));
                add(new FlugRow(19, "DB", 6, "ERF", "FRA", 915, 945));
                add(new FlugRow(75, "DB", 7, "FRA", "ERF", 1000, 1030));
                add(new FlugRow(20, "DB", 8, "MIR", "FRA", 1000, 1230));
                add(new FlugRow(21, "DB", 9, "FRA", "MIR", 1500, 1730));
                add(new FlugRow(67, "DB", 10, "DJE", "FRA", 1000, 1230));
                add(new FlugRow(6, "DB", 11, "FRA", "DJE", 1500, 1730));
                add(new FlugRow(76, "DB", 14, "LYS", "FRA", 1845, 1955));
                add(new FlugRow(37, "DB", 15, "FRA", "LYS", 115, 230));
                add(new FlugRow(38, "DB", 16, "PMI", "FRA", 455, 650));
                add(new FlugRow(68, "DB", 17, "FRA", "PMI", 915, 1150));
                add(new FlugRow(93, "DB", 18, "DRS", "FRA", 600, 700));
                add(new FlugRow(77, "DB", 19, "FRA", "DRS", 1750, 1850));

                add(new FlugRow(39, "DB", 20, "ALF", "FRA", 630, 700));
                add(new FlugRow(78, "DB", 21, "FRA", "ALF", 930, 1050));
                add(new FlugRow(86, "DB", 22, "PEK", "FRA", 0, 800));
                add(new FlugRow(53, "DB", 23, "FRA", "PEK", 1500, 2300));
                add(new FlugRow(40, "DB", 24, "KLU", "FRA", 1050, 1250));
                add(new FlugRow(22, "DB", 25, "FRA", "KLU", 1450, 1650));
                add(new FlugRow(23, "DB", 28, "TUN", "FRA", 1000, 1230));
                add(new FlugRow(24, "DB", 29, "FRA", "TUN", 1500, 1730));
                add(new FlugRow(94, "DB", 32, "MPL", "FRA", 820, 1050));
                add(new FlugRow(69, "DB", 33, "FRA", "MPL", 1210, 1440));
                add(new FlugRow(25, "DB", 36, "HAJ", "FRA", 800, 900));
                add(new FlugRow(26, "DB", 37, "FRA", "HAJ", 1200, 1300));
                add(new FlugRow(95, "DB", 40, "YYC", "FRA", 815, 1715));

                add(new FlugRow(27, "DB", 41, "FRA", "YYC", 1830, 330));
                add(new FlugRow(28, "DB", 42, "SSH", "FRA", 500, 900));
                add(new FlugRow(29, "DB", 43, "FRA", "SSH", 1100, 1400));
                add(new FlugRow(7, "DB", 46, "LIM", "FRA", 2055, 1830));
                add(new FlugRow(79, "DB", 47, "FRA", "LIM", 2015, 820));
                add(new FlugRow(8, "DB", 48, "DUS", "FRA", 915, 1000));
                add(new FlugRow(41, "DB", 49, "FRA", "DUS", 1115, 1200));
                add(new FlugRow(96, "DB", 54, "SVO", "FRA", 1620, 1745));
                add(new FlugRow(80, "DB", 55, "FRA", "SVO", 1845, 2010));
                add(new FlugRow(87, "DB", 56, "BRE", "FRA", 615, 700));
                add(new FlugRow(9, "DB", 57, "FRA", "BRE", 800, 845));
                add(new FlugRow(54, "DB", 58, "LPA", "FRA", 100, 550));
                add(new FlugRow(10, "DB", 59, "FRA", "LPA", 1700, 2150));

                add(new FlugRow(42, "DB", 60, "ISA", "FRA", 415, 32));
                add(new FlugRow(43, "DB", 61, "FRA", "ISA", 1750, 909));
                add(new FlugRow(55, "DB", 62, "HKG", "FRA", 1000, 1700));
                add(new FlugRow(56, "DB", 63, "FRA", "HKG", 1900, 800));
                add(new FlugRow(11, "DB", 72, "YDE", "FRA", 2315, 725));
                add(new FlugRow(57, "DB", 73, "FRA", "YDE", 1740, 2340));
                add(new FlugRow(58, "DB", 74, "CGK", "FRA", 2300, 1000));
                add(new FlugRow(12, "DB", 75, "FRA", "CGK", 1700, 1300));
                add(new FlugRow(70, "DB", 76, "MVR", "FRA", 1000, 1700));
                add(new FlugRow(59, "DB", 77, "FRA", "MVR", 1800, 100));
                add(new FlugRow(44, "DB", 78, "TLS", "FRA", 830, 1000));
                add(new FlugRow(71, "DB", 79, "FRA", "TLS", 1230, 1400));
                add(new FlugRow(72, "DB", 80, "SZG", "FRA", 800, 1000));

                add(new FlugRow(45, "DB", 81, "FRA", "SZG", 1200, 1400));
                add(new FlugRow(60, "DB", 82, "CRT", "FRA", 1000, 1230));
                add(new FlugRow(61, "DB", 83, "FRA", "CRT", 1500, 1730));
                add(new FlugRow(46, "DB", 90, "TRU", "FRA", 2200, 1020));
                add(new FlugRow(30, "DB", 91, "FRA", "TRU", 1430, 2200));
                add(new FlugRow(13, "DL", 7, "ORD", "SFO", 1140, 1530));
                add(new FlugRow(14, "DL", 9, "LAX", "NRT", 2220, 1000));
                add(new FlugRow(97, "DL", 33, "SFO", "LAX", 900, 1005));
                add(new FlugRow(98, "JL", 12, "TXL", "KIX", 1355, 820));
                add(new FlugRow(31, "LH", 5, "FRA", "TXL", 730, 830));
                add(new FlugRow(62, "LH", 6, "TXL", "FRA", 930, 1030));
                add(new FlugRow(88, "LH", 7, "FRA", "TXL", 1130, 1230));
                add(new FlugRow(63, "LH", 8, "TXL", "FRA", 1230, 1330));

                add(new FlugRow(47, "LH", 9, "FRA", "TXL", 1430, 1530));
                add(new FlugRow(48, "LH", 10, "TXL", "FRA", 1630, 1730));
                add(new FlugRow(15, "LH", 20, "TXL", "CDG", 900, 1140));
                add(new FlugRow(32, "LH", 24, "TXL", "FRA", 2130, 2310));
                add(new FlugRow(89, "LH", 32, "JFK", "LAX", 1400, 1800));
                add(new FlugRow(16, "LH", 34, "FRA", "SFO", 1015, 1245));
                add(new FlugRow(49, "LH", 36, "SFO", "LAX", 1700, 1815));
                add(new FlugRow(90, "LH", 40, "FRA", "LHR", 700, 815));
                add(new FlugRow(99, "LH", 41, "LHR", "FRA", 1025, 1145));
                add(new FlugRow(50, "LH", 42, "FRA", "LHR", 1300, 1415));
                add(new FlugRow(81, "LH", 43, "LHR", "FRA", 1515, 1640));
                add(new FlugRow(64, "LH", 44, "FRA", "LHR", 1600, 1715));
                add(new FlugRow(17, "LH", 45, "LHR", "FRA", 1830, 2000));

                add(new FlugRow(33, "LH", 46, "FRA", "LHR", 2130, 2245));
                add(new FlugRow(51, "LH", 47, "LHR", "FRA", 700, 820));
                add(new FlugRow(34, "LH", 50, "FRA", "CDG", 710, 810));
                add(new FlugRow(100, "LH", 51, "CDG", "FRA", 930, 1030));
                add(new FlugRow(82, "LH", 52, "FRA", "CDG", 1210, 1310));
                add(new FlugRow(35, "LH", 53, "CDG", "FRA", 1500, 1620));
                add(new FlugRow(73, "LH", 54, "FRA", "CDG", 1740, 1850));
                add(new FlugRow(83, "LH", 55, "CDG", "FRA", 2000, 2120));
                add(new FlugRow(65, "LH", 60, "FRA", "BHX", 1005, 1130));
            }
        };

        return rows;
    }

    protected HashMap<Integer, FlugRow> getFlugRowsAsPKHashMap() throws FedException {
        HashMap<Integer, FlugRow> map = new HashMap<>();
        for (FlugRow flug : getFlugRows()) {
            map.put(flug.getInt(1), flug);
        }

        return map;
    }

    protected void insertFlugVonUnqieRows() throws FedException {
        executeUpdate("INSERT INTO FLUG VALUES (91, 'AC', 10, 'YYZ', 'FRA', 1815, 740)");
        executeUpdate("INSERT INTO FLUG VALUES (52, 'AC', 11, 'YUL', 'YYZ', 1500, 1700)");
        executeUpdate("INSERT INTO FLUG VALUES (18, 'AF', 9, 'TXL', 'CDG', 915, 1040)");
        executeUpdate("INSERT INTO FLUG VALUES (74, 'AF', 33, 'FRA', 'CDG', 900, 1010)");
        executeUpdate("INSERT INTO FLUG VALUES (1, 'AF', 35, 'CDG', 'FRA', 1400, 1500)");
        executeUpdate("INSERT INTO FLUG VALUES (2, 'BA', 84, 'ORD', 'SFO', 1530, 1800)");
        executeUpdate("INSERT INTO FLUG VALUES (3, 'BA', 87, 'LHR', 'FRA', 1310, 1450)");
        executeUpdate("INSERT INTO FLUG VALUES (5, 'BA', 90, 'LGW', 'BHX', 1020, 1150)");
        executeUpdate("INSERT INTO FLUG VALUES (36, 'DB', 2, 'HHN', 'FRA', 1120, 1230)");
        executeUpdate("INSERT INTO FLUG VALUES (67, 'DB', 10, 'DJE', 'FRA', 1000, 1230)");
        executeUpdate("INSERT INTO FLUG VALUES (76, 'DB', 14, 'LYS', 'FRA', 1845, 1955)");
        executeUpdate("INSERT INTO FLUG VALUES (38, 'DB', 16, 'PMI', 'FRA', 455, 650)");
        executeUpdate("INSERT INTO FLUG VALUES (93, 'DB', 18, 'DRS', 'FRA', 600, 700)");
        executeUpdate("INSERT INTO FLUG VALUES (39, 'DB', 20, 'ALF', 'FRA', 630, 700)");
        executeUpdate("INSERT INTO FLUG VALUES (86, 'DB', 22, 'PEK', 'FRA', 0, 800)");
        executeUpdate("INSERT INTO FLUG VALUES (40, 'DB', 24, 'KLU', 'FRA', 1050, 1250)");
        executeUpdate("INSERT INTO FLUG VALUES (23, 'DB', 28, 'TUN', 'FRA', 1000, 1230)");
        executeUpdate("INSERT INTO FLUG VALUES (94, 'DB', 32, 'MPL', 'FRA', 820, 1050)");
        executeUpdate("INSERT INTO FLUG VALUES (25, 'DB', 36, 'HAJ', 'FRA', 800, 900)");
        executeUpdate("INSERT INTO FLUG VALUES (95, 'DB', 40, 'YYC', 'FRA', 815, 1715)");
        executeUpdate("INSERT INTO FLUG VALUES (28, 'DB', 42, 'SSH', 'FRA', 500, 900)");
        executeUpdate("INSERT INTO FLUG VALUES (7, 'DB', 46, 'LIM', 'FRA', 2055, 1830)");
        executeUpdate("INSERT INTO FLUG VALUES (8, 'DB', 48, 'DUS', 'FRA', 915, 1000)");
        executeUpdate("INSERT INTO FLUG VALUES (96, 'DB', 54, 'SVO', 'FRA', 1620, 1745)");
        executeUpdate("INSERT INTO FLUG VALUES (87, 'DB', 56, 'BRE', 'FRA', 615, 700)");
        executeUpdate("INSERT INTO FLUG VALUES (54, 'DB', 58, 'LPA', 'FRA', 100, 550)");
    }

    protected ArrayList<FlugRow> getFlugVonUnqieRows() {
        ArrayList<FlugRow> rows = new ArrayList<FlugRow>() {
            {
                add(new FlugRow(91, "AC", 10, "YYZ", "FRA", 1815, 740));
                add(new FlugRow(52, "AC", 11, "YUL", "YYZ", 1500, 1700));
                add(new FlugRow(18, "AF", 9, "TXL", "CDG", 915, 1040));
                add(new FlugRow(74, "AF", 33, "FRA", "CDG", 900, 1010));
                add(new FlugRow(1, "AF", 35, "CDG", "FRA", 1400, 1500));
                add(new FlugRow(2, "BA", 84, "ORD", "SFO", 1530, 1800));
                add(new FlugRow(3, "BA", 87, "LHR", "FRA", 1310, 1450));
                add(new FlugRow(5, "BA", 90, "LGW", "BHX", 1020, 1150));
                add(new FlugRow(36, "DB", 2, "HHN", "FRA", 1120, 1230));
                add(new FlugRow(67, "DB", 10, "DJE", "FRA", 1000, 1230));
                add(new FlugRow(76, "DB", 14, "LYS", "FRA", 1845, 1955));
                add(new FlugRow(38, "DB", 16, "PMI", "FRA", 455, 650));
                add(new FlugRow(93, "DB", 18, "DRS", "FRA", 600, 700));
                add(new FlugRow(39, "DB", 20, "ALF", "FRA", 630, 700));
                add(new FlugRow(86, "DB", 22, "PEK", "FRA", 0, 800));
                add(new FlugRow(40, "DB", 24, "KLU", "FRA", 1050, 1250));
                add(new FlugRow(23, "DB", 28, "TUN", "FRA", 1000, 1230));
                add(new FlugRow(94, "DB", 32, "MPL", "FRA", 820, 1050));
                add(new FlugRow(25, "DB", 36, "HAJ", "FRA", 800, 900));
                add(new FlugRow(95, "DB", 40, "YYC", "FRA", 815, 1715));
                add(new FlugRow(28, "DB", 42, "SSH", "FRA", 500, 900));
                add(new FlugRow(7, "DB", 46, "LIM", "FRA", 2055, 1830));
                add(new FlugRow(8, "DB", 48, "DUS", "FRA", 915, 1000));
                add(new FlugRow(96, "DB", 54, "SVO", "FRA", 1620, 1745));
                add(new FlugRow(87, "DB", 56, "BRE", "FRA", 615, 700));
                add(new FlugRow(54, "DB", 58, "LPA", "FRA", 100, 550));
            }
        };

        return rows;
    }

    protected HashMap<String, FlugRow> getFlugVonUnqieRowsAsPKHashMap() throws FedException {
        HashMap<String, FlugRow> map = new HashMap<>();
        for (FlugRow flug : getFlugVonUnqieRows()) {
            map.put(flug.getString(4), flug);
        }

        return map;
    }
}
