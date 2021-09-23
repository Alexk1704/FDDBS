/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.meta;

import fdbs.sql.FedException;
import fdbs.sql.meta.BetweenConstraint;
import fdbs.sql.meta.BinaryConstraint;
import fdbs.sql.meta.BinaryType;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.ComparisonOperator;
import fdbs.sql.meta.Constraint;
import fdbs.sql.meta.ConstraintType;
import fdbs.sql.meta.HorizontalPartition;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.meta.PartitionType;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.util.logger.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
//import static org.junit.Assert.*;

/**
 *
 * @author Nicolai
 */
public class ManagerTest extends TestCase {

    private MetadataManager manager;
    
    public ManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test Manager class:");
    }

    @After
    public void tearDown() {
        Logger.infoln("End of Manager class test.");
    }

    @Test
    public void testManager() throws ParseException, FedException, IOException {
        manager = ManagerTestUtils.SetUpDatabase("VDBSA01", "VDBSA01");
        
        //Test manager.getAllTables method
        Assert.assertNotNull("getallTables should be not null", manager.getAllTables());

        // Test getAllColumns, getAllTables, getAllConstraints not null
        Assert.assertNotNull("Columns should be not null", manager.getAllColumns().size());
        Assert.assertNotNull("Tables should be not null", manager.getAllTables().size());
        Assert.assertNotNull("Constraints should be not null", manager.getAllConstraints().size());

        // And for (createMetadataTable)
        Assert.assertTrue("Columns should be 45", 45 == manager.getAllColumns().size());
        Assert.assertTrue("Tables should be 10", 10 == manager.getAllTables().size());
        Assert.assertTrue("Constraints should be 82", 82 == manager.getAllConstraints().size());
        
        // check tables, columns, constraints, horizontals
        this.checkTables();
        this.checkColumns();
        this.checkHorizontals();
        this.checkPrimaryKeyConstraints();
        this.checkForeignKeyConstraints();
        this.checkUniqueConstrains();
        this.checkNotNullConstraints();
        this.checkBetweenConstraints();
        this.checkBinaryConstraints();
        
        // Test Drop
        SqlParser parser = new SqlParser("DROP TABLE MITARBEITER");
        AST ast = parser.parseStatement();
        manager.handleQuery(ast);
        
        parser = new SqlParser("DROP TABLE ADRESSE");
        ast = parser.parseStatement();
        manager.handleQuery(ast);
        
        Assert.assertTrue("There should be 78 Tables after drop.", manager.getTablesByName().size() == 8);
        Assert.assertNull("There should be no MITARBEITER table", manager.getTable("MITARBEITER"));
        Assert.assertNull("There should be no ADRESSE table.", manager.getTable("ADRESSE"));
        Assert.assertTrue("There should be 37 columns after drop.", manager.getAllColumns().size() == 37);
        Assert.assertTrue("There should be 61 constrains after drop.", manager.getAllConstraints().size() == 61);
    }    
    
    /**
     * Methode zum Überprüfen der horizontalen Partitionen.
     */
    private void checkHorizontals()
    {
        // check horizontal partitions
        ArrayList<HorizontalPartition> horizontalFlugzeug = manager.getHorizontalPartition("FLUGZEUG");
        ArrayList<HorizontalPartition> horizontalPassagier = manager.getHorizontalPartition("PASSAGIER");
        ArrayList<HorizontalPartition> horizontalPersonen = manager.getHorizontalPartition("PERSON");
        ArrayList<HorizontalPartition> horizontalAdresse = manager.getHorizontalPartition("ADRESSE");

        Assert.assertTrue("Flugzeug should have 2 horizontal partitions", horizontalFlugzeug.size() == 2);
        Assert.assertTrue("Passagier should have 3 horizontal partitions", horizontalPassagier.size() == 3);
        Assert.assertTrue("Personen should have 3 horizontal partitions", horizontalPersonen.size() == 3);
        Assert.assertTrue("Adresse should have 2 horizontal partitions", horizontalAdresse.size() == 2);

        Assert.assertTrue("FLUGZEUG horizontal 1 should be on db1", horizontalFlugzeug.get(0).getDatabase() == 1);
        Assert.assertTrue("FLUGZEUG horizontal 2 should be on db2", horizontalFlugzeug.get(1).getDatabase() == 2);
        Assert.assertTrue("FLUGZEUG horizontal 1 should have attribute FNR", horizontalFlugzeug.get(0).getAttributeName().equals("FNR"));
        Assert.assertTrue("FLUGZEUG horizontal 2 should have attribute FNR", horizontalFlugzeug.get(1).getAttributeName().equals("FNR"));
        Assert.assertTrue("FLUGZEUG horizontal 1 should have type INTEGER", horizontalFlugzeug.get(0).getType().equals("INTEGER"));
        Assert.assertTrue("FLUGZEUG horizontal 2 should have type INTEGER", horizontalFlugzeug.get(1).getType().equals("INTEGER"));
        Assert.assertTrue("FLUGZEUG horizontal 1 should have low null", horizontalFlugzeug.get(0).getLow() == null);
        Assert.assertTrue("FLUGZEUG horizontal 1 should have high 40", horizontalFlugzeug.get(0).getHigh().equals("40"));
        Assert.assertTrue("FLUGZEUG horizontal 1 have default 40", horizontalFlugzeug.get(0).getDefaultValue().equals("40"));
        Assert.assertTrue("FLUGZEUG horizontal 2 should have low 41", horizontalFlugzeug.get(1).getLow().equals("41"));
        Assert.assertTrue("FLUGZEUG horizontal 2 should have high null", horizontalFlugzeug.get(1).getHigh() == null);
        Assert.assertTrue("FLUGZEUG horizontal 2 have default 40", horizontalFlugzeug.get(1).getDefaultValue().equals("41"));

        Assert.assertTrue("Passagier horizontal 1 should be on db1", horizontalPassagier.get(0).getDatabase() == 1);
        Assert.assertTrue("Passagier horizontal 2 should be on db2", horizontalPassagier.get(1).getDatabase() == 2);
        Assert.assertTrue("Passagier horizontal 3 should be on db3", horizontalPassagier.get(2).getDatabase() == 3);
        Assert.assertTrue("Passagier horizontal 1 should have attribute PNR", horizontalPassagier.get(0).getAttributeName().equals("PNR"));
        Assert.assertTrue("Passagier horizontal 2 should have attribute PNR", horizontalPassagier.get(1).getAttributeName().equals("PNR"));
        Assert.assertTrue("Passagier horizontal 3 should have attribute PNR", horizontalPassagier.get(2).getAttributeName().equals("PNR"));
        Assert.assertTrue("Passagier horizontal 1 should have type INTEGER", horizontalPassagier.get(0).getType().equals("INTEGER"));
        Assert.assertTrue("Passagier horizontal 2 should have type INTEGER", horizontalPassagier.get(1).getType().equals("INTEGER"));
        Assert.assertTrue("Passagier horizontal 3 should have type INTEGER", horizontalPassagier.get(2).getType().equals("INTEGER"));
        Assert.assertTrue("Passagier horizontal 1 should have low null", horizontalPassagier.get(0).getLow() == null);
        Assert.assertTrue("Passagier horizontal 1 should have high 35", horizontalPassagier.get(0).getHigh().equals("35"));
        Assert.assertTrue("Passagier horizontal 1 should have default 35", horizontalPassagier.get(0).getDefaultValue().equals("35"));
        Assert.assertTrue("Passagier horizontal 2 should have low 36", horizontalPassagier.get(1).getLow().equals("36"));
        Assert.assertTrue("Passagier horizontal 2 should have high 70", horizontalPassagier.get(1).getHigh().equals("70"));
        Assert.assertTrue("Passagier horizontal 2 should have default 70", horizontalPassagier.get(1).getDefaultValue().equals("70"));
        Assert.assertTrue("Passagier horizontal 3 should have low 71", horizontalPassagier.get(2).getLow().equals("71"));
        Assert.assertTrue("Passagier horizontal 3 should have high null", horizontalPassagier.get(2).getHigh() == null);
        Assert.assertTrue("Passagier horizontal 3 should have default 70", horizontalPassagier.get(2).getDefaultValue().equals("71"));

        Assert.assertTrue("Person horizontal 1 should be on db1", horizontalPersonen.get(0).getDatabase() == 1);
        Assert.assertTrue("Person horizontal 2 should be on db2", horizontalPersonen.get(1).getDatabase() == 2);
        Assert.assertTrue("Person horizontal 3 should be on db3", horizontalPersonen.get(2).getDatabase() == 3);
        Assert.assertTrue("Person horizontal 1 should have attribute STATE", horizontalPersonen.get(0).getAttributeName().equals("STATE"));
        Assert.assertTrue("Person horizontal 2 should have attribute STATE", horizontalPersonen.get(1).getAttributeName().equals("STATE"));
        Assert.assertTrue("Person horizontal 3 should have attribute STATE", horizontalPersonen.get(2).getAttributeName().equals("STATE"));
        Assert.assertTrue("Person horizontal 1 should have type VARCHAR (40)", horizontalPersonen.get(0).getType().equals("VARCHAR(40)"));
        Assert.assertTrue("Person horizontal 2 should have type VARCHAR (40)", horizontalPersonen.get(1).getType().equals("VARCHAR(40)"));
        Assert.assertTrue("Person horizontal 3 should have type VARCHAR (40)", horizontalPersonen.get(2).getType().equals("VARCHAR(40)"));
        Assert.assertTrue("Person horizontal 1 should have low null", horizontalPersonen.get(0).getLow() == null);
        Assert.assertTrue("Person horizontal 1 should have high CT", horizontalPersonen.get(0).getHigh().equals("CT"));
        Assert.assertTrue("Person horizontal 1 should have default CT", horizontalPersonen.get(0).getDefaultValue().equals("CT"));
        Assert.assertTrue("Person horizontal 2 should have low CT", horizontalPersonen.get(1).getLow().equals("CT"));
        Assert.assertTrue("Person horizontal 2 should have high TX", horizontalPersonen.get(1).getHigh().equals("TX"));
        Assert.assertTrue("Person horizontal 2 should have default CU", horizontalPersonen.get(1).getDefaultValue().equals("CU"));
        Assert.assertTrue("Person horizontal 3 should have low TX", horizontalPersonen.get(2).getLow().equals("TX"));
        Assert.assertTrue("Person horizontal 3 should have high null", horizontalPersonen.get(2).getHigh() == null);
        Assert.assertTrue("Person horizontal 3 should have default TY", horizontalPersonen.get(2).getDefaultValue().equals("TY"));

        Assert.assertTrue("Adresse horizontal 1 should be on db1", horizontalAdresse.get(0).getDatabase() == 1);
        Assert.assertTrue("Adresse horizontal 2 should be on db2", horizontalAdresse.get(1).getDatabase() == 2);
        Assert.assertTrue("Adresse horizontal 1 should have attribute STATE", horizontalAdresse.get(0).getAttributeName().equals("STATE"));
        Assert.assertTrue("Adresse horizontal 2 should have attribute STATE", horizontalAdresse.get(1).getAttributeName().equals("STATE"));
        Assert.assertTrue("Adresse horizontal 1 should have type VARCHAR (40)", horizontalAdresse.get(0).getType().equals("VARCHAR(40)"));
        Assert.assertTrue("Adresse horizontal 2 should have type VARCHAR (40)", horizontalAdresse.get(1).getType().equals("VARCHAR(40)"));
        Assert.assertTrue("Adresse horizontal 1 should have low null", horizontalAdresse.get(0).getLow() == null);
        Assert.assertTrue("Adresse horizontal 1 should have high CM", horizontalAdresse.get(0).getHigh().equals("CM"));
        Assert.assertTrue("Adresse horizontal 1 should have default CM", horizontalAdresse.get(0).getDefaultValue().equals("CM"));
        Assert.assertTrue("Adresse horizontal 2 should have low CM", horizontalAdresse.get(1).getLow().equals("CM"));
        Assert.assertTrue("Adresse horizontal 2 should have high null", horizontalAdresse.get(1).getHigh() == null);
        Assert.assertTrue("Adresse horizontal 2 should have default CN", horizontalAdresse.get(1).getDefaultValue().equals("CN"));
    }
    
    /**
     * Methode zum Überprüfen der Columns.
     */
    private void checkColumns()
    {
                // Test vertical Partitions
        Column columnMid = manager.getColumn("MITARBEITER", "MID");
        Column columnState = manager.getColumn("MITARBEITER", "STATE");
        Column columnName = manager.getColumn("MITARBEITER", "NAME");
        Column columnLand = manager.getColumn("MITARBEITER", "LAND");

        Assert.assertTrue("ColumnMid should be on table 15", columnMid.getTableId() == 15);
        Assert.assertTrue("ColumnMid should be on db1", columnMid.getDatabase() == 1);
        Assert.assertTrue("ColumnMid should have no defaultValue", columnMid.getDefaultValue() == null);
        Assert.assertTrue("ColumnMid should name MID", columnMid.getAttributeName().equals("MID"));
        Assert.assertTrue("ColumnMid should be type INTEGER", columnMid.getType().equals("INTEGER"));

        Assert.assertTrue("columnState should be on table 15", columnState.getTableId() == 15);
        Assert.assertTrue("columnState should be on db1", columnState.getDatabase() == 1);
        Assert.assertTrue("columnState should have no defaultValue", columnState.getDefaultValue() == null);
        Assert.assertTrue("columnState should name STATE", columnState.getAttributeName().equals("STATE"));
        Assert.assertTrue("columnState should be type VARCHAR (40)", columnState.getType().equals("VARCHAR(40)"));
        
        Assert.assertTrue("columnName should be on table 15", columnName.getTableId() == 15);
        Assert.assertTrue("columnName should be on db2", columnName.getDatabase() == 2);
        Assert.assertTrue("columnName should have no defaultValue", columnName.getDefaultValue() == null);
        Assert.assertTrue("columnName should name NAME", columnName.getAttributeName().equals("NAME"));
        Assert.assertTrue("columnName should be type VARCHAR (50)", columnName.getType().equals("VARCHAR(50)"));
        
        Assert.assertTrue("columnLand should be on table 15", columnLand.getTableId() == 15);
        Assert.assertTrue("columnLand should be on db2", columnLand.getDatabase() == 3);
        Assert.assertTrue("columnLand should have default Value DE", columnLand.getDefaultValue().equals("DE"));
        Assert.assertTrue("columnLand should name NAME", columnLand.getAttributeName().equals("LAND"));
        Assert.assertTrue("columnLand should be type VARCHAR (50)", columnLand.getType().equals("VARCHAR(100)"));
        
        Assert.assertTrue("MITARBEITER should have 2 columns on db1", manager.getColumnsByDatabase("MITARBEITER", 1).size() == 2);
        Assert.assertTrue("MITARBEITER should have 1 columns on db2", manager.getColumnsByDatabase("MITARBEITER", 2).size() == 1);
        Assert.assertTrue("MITARBEITER should have 2 columns on db3", manager.getColumnsByDatabase("MITARBEITER", 3).size() == 2);
        Assert.assertTrue("MITARBEITER should have 4 columns", manager.getColumns("MITARBEITER").size() == 5);
        Assert.assertTrue("MITARBEITER should have 5 columns on different dbs", manager.getVerticalPartition("MITARBEITER").size() == 5);

        Column columnFHC = manager.getColumn("FLUGHAFEN", "FHC");
        Column columnLandFlughafen = manager.getColumn("FLUGHAFEN", "LAND");
        Column columnStadtFlughafen = manager.getColumn("FLUGHAFEN", "STADT");
        Column columnNameFlughafen = manager.getColumn("FLUGHAFEN", "NAME");

        Assert.assertTrue("columnFHC should 6", columnFHC.getTableId() == 6);
        Assert.assertTrue("columnFHC should be on db1", columnFHC.getDatabase() == 2);
        Assert.assertTrue("columnFHC should have no defaultValue", columnFHC.getDefaultValue() == null);
        Assert.assertTrue("columnFHC should name FHC", columnFHC.getAttributeName().equals("FHC"));
        Assert.assertTrue("columnFHC should be type VARCHAR(3)", columnFHC.getType().equals("VARCHAR(3)"));

        Assert.assertTrue("columnLandFlughafen should have tableid 6", columnLandFlughafen.getTableId() == 6);
        Assert.assertTrue("columnLandFlughafen should be on db1", columnLandFlughafen.getDatabase() == 1);
        Assert.assertTrue("columnLandFlughafen should have no defaultValue", columnLandFlughafen.getDefaultValue() == null);
        Assert.assertTrue("columnLandFlughafen should name LAND", columnLandFlughafen.getAttributeName().equals("LAND"));

        Assert.assertTrue("columnLandFlughafen should be type VARCHAR (40)", columnLandFlughafen.getType().equals("VARCHAR(3)"));
        Assert.assertTrue("columnStadtFlughafen should have tableid 6", columnStadtFlughafen.getTableId() == 6);
        Assert.assertTrue("columnStadtFlughafen should be on db2", columnStadtFlughafen.getDatabase() == 2);
        Assert.assertTrue("columnStadtFlughafen should have Berlin as default value", columnStadtFlughafen.getDefaultValue().equals("Berlin"));
        Assert.assertTrue("columnStadtFlughafen should name STADT", columnStadtFlughafen.getAttributeName().equals("STADT"));
        Assert.assertTrue("columnStadtFlughafen should be type VARCHAR (50)", columnStadtFlughafen.getType().equals("VARCHAR(50)"));
        
        Assert.assertTrue("columnNameFlughafen should have tableid 6", columnNameFlughafen.getTableId() == 6);
        Assert.assertTrue("columnNameFlughafen should be on db2", columnNameFlughafen.getDatabase() == 2);
        Assert.assertTrue("columnNameFlughafen should have no default value", columnNameFlughafen.getDefaultValue() == null);
        Assert.assertTrue("columnNameFlughafen should name NAME", columnNameFlughafen.getAttributeName().equals("NAME"));
        Assert.assertTrue("columnNameFlughafen should have type VARCHAR (50)", columnNameFlughafen.getType().equals("VARCHAR(50)"));
        
        Assert.assertTrue("FLUGHAFEN should have 3 columns on db1", manager.getColumnsByDatabase("FLUGHAFEN", 1).size() == 2);
        Assert.assertTrue("FLUGHAFEN should have 2 columns on db2", manager.getColumnsByDatabase("FLUGHAFEN", 2).size() == 3);
        Assert.assertTrue("FLUGHAFEN should have 5 columns", manager.getColumns("FLUGHAFEN").size() == 5);
        Assert.assertTrue("columnNameFlughafen should have type VARCHAR (50)", manager.getColumnType("FLUGHAFEN", "NAME").equals("VARCHAR(50)"));
        
        Assert.assertTrue("FLUGHAFEN should have 5 columns on different dbs", manager.getVerticalPartition("FLUGHAFEN").size() == 5);

        Column columnNameFlugzeug = manager.getColumn("FLUGZEUG", "NAME");
        Assert.assertTrue("columnNameFlugzeug should be on tableId 9", columnNameFlugzeug.getTableId() == 9);
        Assert.assertTrue("columnNameFlugzeug should be on no database", columnNameFlugzeug.getDatabase() == 0);
        Assert.assertTrue("columnNameFlugzeug should have no default value", columnNameFlugzeug.getDefaultValue().equals("Airbus"));
        Assert.assertTrue("columnNameFlugzeug should name NAME", columnNameFlugzeug.getAttributeName().equals("NAME"));
        Assert.assertTrue("columnNameFlugzeug should be type VARCHAR (50)", columnNameFlugzeug.getType().equals("VARCHAR(40)"));
        
        // check tables
        Table flughafen = manager.getTable("FLUGHAFEN");
        Table mitarbeiter = manager.getTable("MITARBEITER");
        
        // Test databaseRelatedColumns
        // vertical on 3 dbs
        HashMap<Integer, LinkedHashMap<String, Column>> columnsMitarbeiter = mitarbeiter.getDatabaseRelatedColumns();
        Assert.assertNotNull("MID should be on first db.", columnsMitarbeiter.get(1).get("MID"));
        Assert.assertNotNull("Name should be on second db.", columnsMitarbeiter.get(2).get("NAME"));
        Assert.assertNotNull("Land should be on thrid db.", columnsMitarbeiter.get(3).get("LAND"));

        // Vertical on 2 dbs
        HashMap<Integer, LinkedHashMap<String, Column>> columnsFlughafen = flughafen.getDatabaseRelatedColumns();
        Assert.assertTrue("columnsFlughafen should have 3 columns on db1.", columnsFlughafen.get(1).size() == 2);
        Assert.assertTrue("columnsFlughafen should have 2 columns on db2.", columnsFlughafen.get(2).size() == 3);
        Assert.assertTrue("columnsFlughafen should have no columns on db3.", columnsFlughafen.get(3).isEmpty());
        
        // no partition
        HashMap<Integer, LinkedHashMap<String, Column>> columnsBuchung = manager.getTable("BUCHUNG").getDatabaseRelatedColumns();
        Assert.assertTrue("columnsBuchung should have 9 columns on db1.", columnsBuchung.get(1).size() == 9);
        Assert.assertTrue("columnsBuchung should have no columns on db2.", columnsBuchung.get(2).isEmpty());
        Assert.assertTrue("columnsBuchung should have no columns on db3.", columnsBuchung.get(3).isEmpty());
        
        // Horizontal on 3
        HashMap<Integer, LinkedHashMap<String, Column>> columnsPassagier = manager.getTable("PASSAGIER").getDatabaseRelatedColumns();
        Assert.assertTrue("columnsPassagier should have all columns on db1.", columnsPassagier.get(1).size() == 5);
        Assert.assertTrue("columnsPassagier should have all columns on db2.", columnsPassagier.get(2).size() == 5);
        Assert.assertTrue("columnsPassagier should have all columns on db3.", columnsPassagier.get(3).size() == 5);
        
        // Horizontal on 2
        HashMap<Integer, LinkedHashMap<String, Column>> columnsFlugzeug = manager.getTable("FLUGZEUG").getDatabaseRelatedColumns();
        Assert.assertTrue("columnsFlugzeug should have all columns on db1.", columnsFlugzeug.get(1).size() == 3);
        Assert.assertTrue("columnsFlugzeug should have all columns on db2.", columnsFlugzeug.get(2).size() == 3);
        Assert.assertTrue("columnsFlugzeug should have no columns on db3.", columnsFlugzeug.get(3).isEmpty());
    }
    
    /**
     * Methode zum Überprüfen der Tables.
     */
    private void checkTables(){
        // check tables
        Table flughafen = manager.getTable("FLUGHAFEN");
        Table mitarbeiter = manager.getTable("MITARBEITER");

        Assert.assertTrue("FLUGHAFEN should be the table name", flughafen.getAttributeName().equals("FLUGHAFEN"));
        Assert.assertTrue("FLUGHAFEN should have tableId 6", flughafen.getId() == 6);
        Assert.assertTrue("FLUGHAFEN should have primary constraint fhc", flughafen.getPrimaryKeyConstraints().get(0).getAttributeName().equals("FHC"));

        Assert.assertTrue("MITARBEITER should be the table name", mitarbeiter.getAttributeName().equals("MITARBEITER"));
        Assert.assertTrue("MITARBEITER should have tableId 15", mitarbeiter.getId() == 15);
        Assert.assertTrue("MITARBEITER should have primary constraint MID", mitarbeiter.getPrimaryKeyConstraints().get(0).getAttributeName().equals("MID"));

        //  Partition Types
        Assert.assertTrue("Table MITARTBEITER should have PartitionType Vertical", manager.getPartitionType("MITARBEITER") == PartitionType.Vertical);
        Assert.assertTrue("Table MITARTBEITER should have PartitionType None", manager.getPartitionType("BUCHUNG") == PartitionType.None);
        Assert.assertTrue("Table MITARTBEITER should have PartitionType Hoirzontal", manager.getPartitionType("PASSAGIER") == PartitionType.Horizontal);
    }
    
    /**
     * Methode zum Überprüfen der PrimaryKey Constraints.
     */
    private void checkPrimaryKeyConstraints(){
        // primarykey integrity checks
        // fluhafen is vertical on 2 => 2PKS
        Constraint primaryConstraintFlughafen1 = manager.getConstraint("FLUGHAFEN", 1, "FLUGHAFEN_PS");
        Constraint primaryConstraintFlughafen2 = manager.getConstraint("FLUGHAFEN", 2, "FLUGHAFEN_PS");
        
        Assert.assertTrue("PrimaryConstraint of Flughafen should have FNR", primaryConstraintFlughafen1.getAttributeName().equals("FHC"));
        Assert.assertTrue("PrimaryConstraint of Flughafen should have DbId 1", primaryConstraintFlughafen1.getDb() == 1);
        Assert.assertTrue("PrimaryConstraint of Flughafen should have NAME FLUGHAFEN_PS", primaryConstraintFlughafen1.getName().equals("FLUGHAFEN_PS"));
        Assert.assertTrue("PrimaryConstraint of Flughafen should have TYPE PK", primaryConstraintFlughafen1.getType() == ConstraintType.PrimaryKey);
        Assert.assertTrue("PrimaryConstraint2 of Flughafen shoud have db2", primaryConstraintFlughafen2.getDb() == 2);
        Assert.assertTrue("Flughafen should only have 2 PK", manager.getTable("FLUGHAFEN").getPrimaryKeyConstraints().size() == 2);

        // Passagier is horizontal on 3 with PK => 3PKS
        Constraint primaryConstraintPassagier1 = manager.getConstraint("PASSAGIER", 1, "PASSAGIER_PS");
        Constraint primaryConstraintPassagier2 = manager.getConstraint("PASSAGIER", 2, "PASSAGIER_PS");
        Constraint primaryConstraintPassagier3 = manager.getConstraint("PASSAGIER", 3, "PASSAGIER_PS");

        Assert.assertTrue("PrimaryConstraint of PASSAGIER should have PNR", primaryConstraintPassagier1.getAttributeName().equals("PNR"));
        Assert.assertTrue("PrimaryConstraint of PASSAGIER should have DbId 1", primaryConstraintPassagier1.getDb() == 1);
        Assert.assertTrue("PrimaryConstraint of PASSAGIER should have NAME PASSAGIER_PS", primaryConstraintPassagier1.getName().equals("PASSAGIER_PS"));
        Assert.assertTrue("PrimaryConstraint of PASSAGIER should have TYPE PK", primaryConstraintPassagier1.getType() == ConstraintType.PrimaryKey);
        Assert.assertTrue("PrimaryConstraint2 of PASSAGIER shoud have db2", primaryConstraintPassagier2.getDb() == 2);
        Assert.assertTrue("PrimaryConstraint3 of PASSAGIER shoud have db3", primaryConstraintPassagier3.getDb() == 3);
        Assert.assertTrue("PASSAGIER should only have 3 PK", manager.getTable("PASSAGIER").getPrimaryKeyConstraints().size() == 3);

        // FLUGZEUG is horizontal on 2 with PK => 2PK
        Constraint primaryConstraintFlugzeug = manager.getConstraint("FLUGZEUG", 1, "FLUGZEUG_PK");
        Constraint primaryConstraintFlugzeug2 = manager.getConstraint("FLUGZEUG", 2, "FLUGZEUG_PK");
        
        Assert.assertTrue("PrimaryConstraint of FLUGZEUG should be PNR", primaryConstraintFlugzeug.getAttributeName().equals("FNR"));
        Assert.assertTrue("PrimaryConstraint of FLUGZEUG should have DbId 1", primaryConstraintFlugzeug.getDb() == 1);
        Assert.assertTrue("PrimaryConstraint of FLUGZEUG should have NAME FLUGZEUG_PK", primaryConstraintFlugzeug.getName().equals("FLUGZEUG_PK"));
        Assert.assertTrue("PrimaryConstraint of FLUGZEUG should have TYPE PK", primaryConstraintFlugzeug.getType() == ConstraintType.PrimaryKey);
        Assert.assertTrue("PrimaryConstraint2 of FLUGZEUG shoud have db2", primaryConstraintFlugzeug2.getDb() == 2);
        Assert.assertTrue("Buchungen should only have 2 PK", manager.getTable("FLUGZEUG").getPrimaryKeyConstraints().size() == 2);
        
        // Buchung is not horizontal so just one PK
        Constraint primaryConstraintBuchung1 = manager.getConstraint("BUCHUNG", 1, "BUCHUNG_PS");
        Assert.assertTrue("PrimaryConstraint of BUCHUNG should be BNR", primaryConstraintBuchung1.getAttributeName().equals("BNR"));
        Assert.assertTrue("PrimaryConstraint of BUCHUNG should have DbId 1", primaryConstraintBuchung1.getDb() == 1);
        Assert.assertTrue("PrimaryConstraint of BUCHUNG should have NAME FLUGZEUG_PS", primaryConstraintBuchung1.getName().equals("BUCHUNG_PS"));
        Assert.assertTrue("PrimaryConstraint of BUCHUNG should have TYPE PK", primaryConstraintBuchung1.getType() == ConstraintType.PrimaryKey);
        Assert.assertTrue("Buchungen should only have 1 PK", manager.getTable("BUCHUNG").getPrimaryKeyConstraints().size() == 1);
        
        // Person is horizontal on 3 without PK => so 3 PKS
        Constraint primaryConstraintPerson = manager.getConstraint("PERSON", 0, "PERSONEN_PID");
        Assert.assertTrue("PrimaryConstraint of PERSON should be PID", primaryConstraintPerson.getAttributeName().equals("PID"));
        Assert.assertTrue("PrimaryConstraint of PERSON should have DbId 0", primaryConstraintPerson.getDb() == 0);
        Assert.assertTrue("PrimaryConstraint of PERSON should have NAME PERSON_PS", primaryConstraintPerson.getName().equals("PERSONEN_PID"));
        Assert.assertTrue("PrimaryConstraint of PERSON should have TYPE PK", primaryConstraintPerson.getType() == ConstraintType.PrimaryKey);
        Assert.assertTrue("PERSON should only have 1 PK", manager.getTable("PERSON").getPrimaryKeyConstraints().size() == 1);
        
        // Adress is horizontal on 2 without PK => so 2PKS
        Constraint primaryConstraintAdresse = manager.getConstraint("ADRESSE", 0, "ADRESSE_AID");
        Assert.assertTrue("PrimaryConstraint of ADRESSE should be PNR", primaryConstraintAdresse.getAttributeName().equals("AID"));
        Assert.assertTrue("PrimaryConstraint of ADRESSE should have DbId 0", primaryConstraintAdresse.getDb() == 0);
        Assert.assertTrue("PrimaryConstraint of ADRESSE should have NAME ADRESSE_AID", primaryConstraintAdresse.getName().equals("ADRESSE_AID"));
        Assert.assertTrue("PrimaryConstraint of ADRESSE should have TYPE PK", primaryConstraintAdresse.getType() == ConstraintType.PrimaryKey);
        Assert.assertTrue("ADRESSE should only have 1 PK", manager.getTable("ADRESSE").getPrimaryKeyConstraints().size() == 1);
        
        // MITARBEITER is vertical on 3 => 3PKS
        Constraint primaryConstraintMitarbeiter = manager.getConstraint("MITARBEITER", 1, "MITARBEITER_MID");
        Constraint primaryConstraintMitarbeiter2 = manager.getConstraint("MITARBEITER", 2, "MITARBEITER_MID");
        Constraint primaryConstraintMitarbeiter3 = manager.getConstraint("MITARBEITER", 3, "MITARBEITER_MID");
        Assert.assertTrue("PrimaryConstraint of MITARBEITER should have PNR", primaryConstraintMitarbeiter.getAttributeName().equals("MID"));
        Assert.assertTrue("PrimaryConstraint of MITARBEITER should have DbId 1", primaryConstraintMitarbeiter.getDb() == 1);
        Assert.assertTrue("PrimaryConstraint of MITARBEITER should have NAME MiTARBEITER_PS", primaryConstraintMitarbeiter.getName().equals("MITARBEITER_MID"));
        Assert.assertTrue("PrimaryConstraint of MITARBEITER should have TYPE PK", primaryConstraintMitarbeiter.getType() == ConstraintType.PrimaryKey);
        Assert.assertTrue("PrimaryConstraint2 of MITARBEITER shoud have db2", primaryConstraintMitarbeiter2.getDb() == 2);
        Assert.assertTrue("PrimaryConstraint3 of MITARBEITER shoud have db3", primaryConstraintMitarbeiter3.getDb() == 3);
        Assert.assertTrue("MITARBEITER should only have 3 PK", manager.getTable("MITARBEITER").getPrimaryKeyConstraints().size() == 3);
    }
    
    /**
     * Methode zum Überprüfen der ForeignKey Constraints.
     */
    private void checkForeignKeyConstraints()
    {
        // check default to default
        Constraint foreignkeyStorn = manager.getConstraint("STORNIERUNG", 1, "STORNIERUNG_FK_SNR");
        Assert.assertTrue("ForeignKeyConstraint should be on db 1", foreignkeyStorn.getDb() == 1);
        Assert.assertTrue("ForeignKeyConstraint should take ATTRIBUTE BNR", foreignkeyStorn.getAttributeName().equals("BNR"));
        Assert.assertTrue("ForeignKeyConstraint should be foreign key type", foreignkeyStorn.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("ForeignKeyConstraint should reference to BUCHUNG", foreignkeyStorn.getTableReference().equals("BUCHUNG"));
        Assert.assertTrue("ForeignKeyConstraint should reference to BNR", foreignkeyStorn.getTableReferenceAttribute().equals("BNR"));
        
        // check default to vertical
        Constraint foreignKeyBuchung = manager.getConstraint("BUCHUNG", 1, "BUCHUNG_FS_VON");
        Assert.assertTrue("foreignKeyBuchung should be on db 1", foreignKeyBuchung.getDb() == 1);
        Assert.assertTrue("foreignKeyBuchung should take ATTRIBUTE VON", foreignKeyBuchung.getAttributeName().equals("VON"));
        Assert.assertTrue("foreignKeyBuchung should be foreign key type", foreignKeyBuchung.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyBuchung should reference to FLUGHAFEN", foreignKeyBuchung.getTableReference().equals("FLUGHAFEN"));
        Assert.assertTrue("foreignKeyBuchung should reference to VON", foreignKeyBuchung.getTableReferenceAttribute().equals("FHC"));

        // vertical to default
        Constraint foreignKeyFlughafen = manager.getConstraint("FLUGHAFEN", 1, "FLUGHAFEN_SID_FK");
        Assert.assertTrue("foreignKeyFlughafen should be on db 1", foreignKeyFlughafen.getDb() == 1);
        Assert.assertTrue("foreignKeyFlughafen should take ATTRIBUTE SID", foreignKeyFlughafen.getAttributeName().equals("SID"));
        Assert.assertTrue("foreignKeyFlughafen should be foreign key type", foreignKeyFlughafen.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyFlughafen should reference to STADT", foreignKeyFlughafen.getTableReference().equals("STADT"));
        Assert.assertTrue("foreignKeyFlughafen should reference to NAME", foreignKeyFlughafen.getTableReferenceAttribute().equals("SID"));
        
        // vertical to vertical on same db
        Constraint foreignKeyChef = manager.getConstraint("CHEF", 2, "CHEF_FK_MID");
        Assert.assertTrue("foreignKeyChef should be on db 3", foreignKeyChef.getDb() == 2);
        Assert.assertTrue("foreignKeyChef should take ATTRIBUTE MID", foreignKeyChef.getAttributeName().equals("MID"));
        Assert.assertTrue("foreignKeyChef should be foreign key type", foreignKeyChef.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyChef should reference to MITARBEITER", foreignKeyChef.getTableReference().equals("MITARBEITER"));
        Assert.assertTrue("foreignKeyChef should reference to MID", foreignKeyChef.getTableReferenceAttribute().equals("MID"));
        
        // vertical to vertical on third db
        Constraint foreignKeyChef3 = manager.getConstraint("CHEF", 3, "CHEF_FK_MID2");
        Assert.assertTrue("foreignKeyChef3 should be on db 3", foreignKeyChef3.getDb() == 3);
        Assert.assertTrue("foreignKeyChef3 should take ATTRIBUTE MID2", foreignKeyChef3.getAttributeName().equals("MID2"));
        Assert.assertTrue("foreignKeyChef3 should be foreign key type", foreignKeyChef3.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyChef3 should reference to MITARBEITER", foreignKeyChef3.getTableReference().equals("MITARBEITER"));
        Assert.assertTrue("foreignKeyChef3 should reference to MID", foreignKeyChef3.getTableReferenceAttribute().equals("MID"));
        
        // horizontal to vertical on 2
        Constraint foreignKeyFlugzeug1 = manager.getConstraint("FLUGZEUG", 1, "FLUGZEUG_FK_FHC");
        Constraint foreignKeyFlugzeug2 = manager.getConstraint("FLUGZEUG", 2, "FLUGZEUG_FK_FHC");
        Constraint foreignKeyFlugzeug3 = manager.getConstraint("FLUGZEUG", 3, "FLUGZEUG_FK_FHC");
        Assert.assertTrue("foreignKeyFlugzeug1 should be on db 1", foreignKeyFlugzeug1.getDb() == 1);
        Assert.assertTrue("foreignKeyFlugzeug1 should take ATTRIBUTE FHC", foreignKeyFlugzeug1.getAttributeName().equals("FHC"));
        Assert.assertTrue("foreignKeyFlugzeug1 should be foreign key type", foreignKeyFlugzeug1.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyFlugzeug1 should reference to FLUGHAFEN", foreignKeyFlugzeug1.getTableReference().equals("FLUGHAFEN"));
        Assert.assertTrue("foreignKeyFlugzeug1 should reference to FHC", foreignKeyFlugzeug1.getTableReferenceAttribute().equals("FHC"));
        Assert.assertTrue("foreignKeyFlugzeug2 should be on db 2", foreignKeyFlugzeug2.getDb() == 2);
        Assert.assertNull("foreignKeyFlugzeug should be null", foreignKeyFlugzeug3);
        
        // horizontal to vertical on 3
        Constraint foreignKeyPerson1 = manager.getConstraint("PERSON", 1, "PERSON_MID_FK");
        Constraint foreignKeyPerson2 = manager.getConstraint("PERSON", 2, "PERSON_MID_FK");
        Constraint foreignKeyPerson3 = manager.getConstraint("PERSON", 3, "PERSON_MID_FK");
        Assert.assertTrue("foreignKeyPerson1 should be on db 1", foreignKeyPerson1.getDb() == 1);
        Assert.assertTrue("foreignKeyPerson1 should take ATTRIBUTE MID", foreignKeyPerson1.getAttributeName().equals("MID"));
        Assert.assertTrue("foreignKeyPerson1 should be foreign key type", foreignKeyPerson1.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyPerson1 should reference to MITARBEITER", foreignKeyPerson1.getTableReference().equals("MITARBEITER"));
        Assert.assertTrue("foreignKeyPerson1 should reference to MID", foreignKeyPerson1.getTableReferenceAttribute().equals("MID"));
        Assert.assertTrue("foreignKeyPerson2 should be on db 2", foreignKeyPerson2.getDb() == 2);
        Assert.assertTrue("foreignKeyPerson3 should be on db 3", foreignKeyPerson3.getDb() == 3);
        
        // check dbId = 0 fk-constraints
        // vertical to default
        Constraint foreignKeyFlughafenStadt = manager.getConstraint("FLUGHAFEN", 0, "FLUGHAFEN_FK_STADT");
        Assert.assertTrue("foreignKeyFlughafenStadt should be on db 0", foreignKeyFlughafenStadt.getDb() == 0);
        Assert.assertTrue("foreignKeyFlughafenStadt should take ATTRIBUTE STADT", foreignKeyFlughafenStadt.getAttributeName().equals("STADT"));
        Assert.assertTrue("foreignKeyFlughafenStadt should be foreign key type", foreignKeyFlughafenStadt.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyFlughafenStadt should reference to STADT", foreignKeyFlughafenStadt.getTableReference().equals("STADT"));
        Assert.assertTrue("foreignKeyFlughafenStadt should reference to NAME", foreignKeyFlughafenStadt.getTableReferenceAttribute().equals("NAME")); 
        
        // vertical to vertical (fk on 3, but second is only on 2)
        Constraint foreignKeyMitarbeiterLand = manager.getConstraint("MITARBEITER", 0, "MITARBEITER_LAND_FK");
        Assert.assertTrue("foreignKeyMitarbeiterLand should be on db 0", foreignKeyMitarbeiterLand.getDb() == 0);
        Assert.assertTrue("foreignKeyMitarbeiterLand should take ATTRIBUTE LAND", foreignKeyMitarbeiterLand.getAttributeName().equals("LAND"));
        Assert.assertTrue("foreignKeyMitarbeiterLand should be foreign key type", foreignKeyMitarbeiterLand.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyMitarbeiterLand should reference to FLUGZEUG", foreignKeyMitarbeiterLand.getTableReference().equals("FLUGHAFEN"));
        Assert.assertTrue("foreignKeyMitarbeiterLand should reference to LAND", foreignKeyMitarbeiterLand.getTableReferenceAttribute().equals("LAND")); 
        
        // horizontal to default
        Constraint foreignKeyPersonBNR = manager.getConstraint("PERSON", 0, "PERSON_FK_BNR");
        Assert.assertTrue("foreignKeyPersonBNR should be on db 0", foreignKeyPersonBNR.getDb() == 0);
        Assert.assertTrue("foreignKeyPersonBNR should take ATTRIBUTE BNR", foreignKeyPersonBNR.getAttributeName().equals("BNR"));
        Assert.assertTrue("foreignKeyPersonBNR should be foreign key type", foreignKeyPersonBNR.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyPersonBNR should reference to BUCHUNG", foreignKeyPersonBNR.getTableReference().equals("BUCHUNG"));
        Assert.assertTrue("foreignKeyPersonBNR should reference to BNR", foreignKeyPersonBNR.getTableReferenceAttribute().equals("BNR"));
        
        // horizontal to vertical (3 on 2)
        Constraint foreignKeyPersonFHC = manager.getConstraint("PERSON", 0, "PERSON_FK_FHR");
        Assert.assertTrue("foreignKeyPersonFHC should be on db 0", foreignKeyPersonFHC.getDb() == 0);
        Assert.assertTrue("foreignKeyPersonFHC should take ATTRIBUTE FHR", foreignKeyPersonFHC.getAttributeName().equals("FHR"));
        Assert.assertTrue("foreignKeyPersonFHC should be foreign key type", foreignKeyPersonFHC.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyPersonFHC should reference to FLUGHAFEN", foreignKeyPersonFHC.getTableReference().equals("FLUGHAFEN"));
        Assert.assertTrue("foreignKeyPersonFHC should reference to FHR", foreignKeyPersonFHC.getTableReferenceAttribute().equals("FHR"));
        
        // horizontal to horizontal
        Constraint foreignKeyPassagierAID = manager.getConstraint("PASSAGIER", 0, "PASSAGIER_FK_AID");
        Assert.assertTrue("foreignKeyPassagierAID should be on db 0", foreignKeyPassagierAID.getDb() == 0);
        Assert.assertTrue("foreignKeyPassagierAID should take ATTRIBUTE AID", foreignKeyPassagierAID.getAttributeName().equals("AID"));
        Assert.assertTrue("foreignKeyPassagierAID should be foreign key type", foreignKeyPassagierAID.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("foreignKeyPassagierAID should reference to ADRESSE", foreignKeyPassagierAID.getTableReference().equals("ADRESSE"));
        Assert.assertTrue("foreignKeyPassagierAID should reference to AID", foreignKeyPassagierAID.getTableReferenceAttribute().equals("AID"));
        
        Constraint foreignkeyConstraint = manager.getConstraint("BUCHUNG", 0, "BUCHUNG_FS_PNR");
        Assert.assertTrue("ForeignKeyConstraint of Buchung should be PNR", foreignkeyConstraint.getAttributeName().equals("PNR"));
        Assert.assertTrue("ForeignKeyConstraint of Buchung should have DbId 0", foreignkeyConstraint.getDb() == 0);
        Assert.assertTrue("ForeignKeyConstraint of Buchung should have NAME FLUGHAFEN_PS", foreignkeyConstraint.getName().equals("BUCHUNG_FS_PNR"));
        Assert.assertTrue("ForeignKeyConstraint of Buchung should have TYPE FK", foreignkeyConstraint.getType() == ConstraintType.ForeignKey);
        Assert.assertTrue("ForeignKeyConstraint of Buchung should have TableRef Passagier", foreignkeyConstraint.getTableReference().equals("PASSAGIER"));
        Assert.assertTrue("ForeignKeyConstraint of Buchung should have TableRefAttribute PNR", foreignkeyConstraint.getTableReferenceAttribute().equals("PNR"));
    }
    
    /**
     * Methode zum Überprüfen der Unique Constrains.
     */
    private void checkUniqueConstrains(){
        // Check unique constraints with integrity
        // check Unique with vertical on 1 
        Constraint uniqueConstraintFLUGHAFEN = manager.getConstraint("FLUGHAFEN", 1, "FHC_UNIQUE_LAND");
        Assert.assertTrue("uniqueConstraint of FLUGHAFEN should have NAME", uniqueConstraintFLUGHAFEN.getAttributeName().equals("LAND"));
        Assert.assertTrue("uniqueConstraint of FLUGHAFEN should have DbId 1", uniqueConstraintFLUGHAFEN.getDb() == 1);
        Assert.assertTrue("uniqueConstraint of FLUGHAFEN should have NAME FHC_UNIQUE_LAND", uniqueConstraintFLUGHAFEN.getName().equals("FHC_UNIQUE_LAND"));
        Assert.assertTrue("uniqueConstraint of FLUGHAFEN should have TYPE U", uniqueConstraintFLUGHAFEN.getType() == ConstraintType.Unique);
        Assert.assertNull("uniqueConstraint of FLUGHAFEN in db 2 shouldnt exist", manager.getConstraint("FLUGHAFEN", 2, "FHC_UNIQUE_LAND"));
        Assert.assertNull("uniqueConstraint of FLUGHAFEN in db 3 shouldnt exist", manager.getConstraint("FLUGHAFEN", 3, "FHC_UNIQUE_LAND"));
        
        // Check horizontal no fixed unqique => db 0 
        Constraint uniqueConstraintFLUGZEUG = manager.getConstraint("FLUGZEUG", 0, "FLUGZEUG_UNIQUE");
        Assert.assertTrue("uniqueConstraint of Flugzeug should have NAME", uniqueConstraintFLUGZEUG.getAttributeName().equals("NAME"));
        Assert.assertTrue("uniqueConstraint of Flugzeug should have DbId 0", uniqueConstraintFLUGZEUG.getDb() == 0);
        Assert.assertTrue("uniqueConstraint of Flugzeug should have NAME FLUGZEUG_UNIQUE", uniqueConstraintFLUGZEUG.getName().equals("FLUGZEUG_UNIQUE"));
        Assert.assertTrue("uniqueConstraint of Flugzeug should have TYPE U", uniqueConstraintFLUGZEUG.getType() == ConstraintType.Unique);
        Assert.assertNull("uniqueConstraint of FLUGZEUG in db 1 shouldnt exist", manager.getConstraint("FLUGZEUG", 1, "FLUGZEUG_UNIQUE"));
        Assert.assertNull("uniqueConstraint of FLUGZEUG in db 2 shouldnt exist", manager.getConstraint("FLUGZEUG", 2, "FLUGZEUG_UNIQUE"));
        Assert.assertNull("uniqueConstraint of FLUGZEUG in db 3 shouldnt exist", manager.getConstraint("FLUGZEUG", 3, "FLUGZEUG_UNIQUE"));

        // CHECK no partition => 1 Unique
        Constraint uniqueConstraintBUCHUNG = manager.getConstraint("BUCHUNG", 1, "BNR_UNIQUE_FLNR");
        Assert.assertTrue("uniqueConstraint of BUCHUNG should have NAME", uniqueConstraintBUCHUNG.getAttributeName().equals("FLNR"));
        Assert.assertTrue("uniqueConstraint of BUCHUNG should have DbId 1", uniqueConstraintBUCHUNG.getDb() == 1);
        Assert.assertTrue("uniqueConstraint of BUCHUNG should have NAME BNR_UNIQUE_FLNR", uniqueConstraintBUCHUNG.getName().equals("BNR_UNIQUE_FLNR"));
        Assert.assertTrue("uniqueConstraint of BUCHUNG should have TYPE U", uniqueConstraintBUCHUNG.getType() == ConstraintType.Unique);
        Assert.assertNull("uniqueConstraint of BUCHUNG in db 2 shouldnt exist", manager.getConstraint("BUCHUNG", 2, "BNR_UNIQUE_FLNR"));
        Assert.assertNull("uniqueConstraint of BUCHUNG in db 3 shouldnt exist", manager.getConstraint("BUCHUNG", 3, "BNR_UNIQUE_FLNR"));
        
        // Check Horizontals on 3 Partitions => 3 Uniques
        Constraint uniqueConstraintPERSON1 = manager.getConstraint("PERSON", 1, "PID_UNIQUE_STATE");
        Constraint uniqueConstraintPERSON2 = manager.getConstraint("PERSON", 2, "PID_UNIQUE_STATE");
        Constraint uniqueConstraintPERSON3 = manager.getConstraint("PERSON", 3, "PID_UNIQUE_STATE");
        
        Assert.assertTrue("uniqueConstraint of PERSON should have NAME", uniqueConstraintPERSON1.getAttributeName().equals("STATE"));
        Assert.assertTrue("uniqueConstraint of PERSON should have DbId 1", uniqueConstraintPERSON1.getDb() == 1);
        Assert.assertTrue("uniqueConstraint of PERSON should have NAME PID_UNIQUE_STATE", uniqueConstraintPERSON1.getName().equals("PID_UNIQUE_STATE"));
        Assert.assertTrue("uniqueConstraint of PERSON should have TYPE U", uniqueConstraintPERSON1.getType() == ConstraintType.Unique);
        Assert.assertTrue("uniqueConstraint2 of PERSON should have DbId 2", uniqueConstraintPERSON2.getDb() == 2);
        Assert.assertTrue("uniqueConstraint3 of PERSON should have DbId 3", uniqueConstraintPERSON3.getDb() == 3);
        
        // Check Horizontal on 2 Partitions => 2 Uniques
        Constraint uniqueConstraintADRESS1 = manager.getConstraint("ADRESSE", 1, "AID_UNIQUE_STATE");
        Constraint uniqueConstraintADRESS2 = manager.getConstraint("ADRESSE", 2, "AID_UNIQUE_STATE");
        Assert.assertTrue("uniqueConstraint of ADRESSE should have NAME", uniqueConstraintADRESS1.getAttributeName().equals("STATE"));
        Assert.assertTrue("uniqueConstraint of ADRESSE should have DbId 1", uniqueConstraintADRESS1.getDb() == 1);
        Assert.assertTrue("uniqueConstraint of ADRESSE should have NAME AID_UNIQUE_STATE", uniqueConstraintADRESS1.getName().equals("AID_UNIQUE_STATE"));
        Assert.assertTrue("uniqueConstraint of ADRESSE should have TYPE U", uniqueConstraintADRESS1.getType() == ConstraintType.Unique);
        Assert.assertTrue("uniqueConstraint2 of ADRESSE should have DbId 2", uniqueConstraintADRESS2.getDb() == 2);
        Assert.assertNull("uniqueConstraint of ADRESSE in db 3 shouldnt exist", manager.getConstraint("ADRESSE", 3, "AID_UNIQUE_STATE"));

        // Check vertical on Partition 2 => 1 Unique on db2
        Constraint uniqueConstraintMITARBEITER = manager.getConstraint("MITARBEITER", 2, "MID_UNIQUE_NAME");
        Assert.assertTrue("uniqueConstraint of MITARBEITER should have NAME", uniqueConstraintMITARBEITER.getAttributeName().equals("NAME"));
        Assert.assertTrue("uniqueConstraint of MITARBEITER should have DbId 2", uniqueConstraintMITARBEITER.getDb() == 2);
        Assert.assertTrue("uniqueConstraint of MITARBEITER should have NAME MID_UNQIUE_NAME", uniqueConstraintMITARBEITER.getName().equals("MID_UNIQUE_NAME"));
        Assert.assertTrue("uniqueConstraint of MITARBEITER should have TYPE U", uniqueConstraintMITARBEITER.getType() == ConstraintType.Unique);
        Assert.assertNull("uniqueConstraint of MITARBEITER in db 1 shouldnt exist", manager.getConstraint("MITARBEITER", 1, "MID_UNIQUE_NAME"));
        Assert.assertNull("uniqueConstraint of MITARBEITER in db 3 shouldnt exist", manager.getConstraint("MITARBEITER", 3, "MID_UNIQUE_NAME"));
    }
    
    /**
     * Methode zum Überprüfen der NotNull Constraints.
     */
    private void checkNotNullConstraints()
    {
        // check constraints
        // check horizontal on 2
        Constraint adresseNullConstraint1 = manager.getConstraint("ADRESSE", 1, "ADRESSE_NAME_NN");
        Constraint adresseNullConstraint2 = manager.getConstraint("ADRESSE", 2, "ADRESSE_NAME_NN");
        Constraint adresseNullConstraint3 = manager.getConstraint("ADRESSE", 3, "ADRESSE_NAME_NN");
        Assert.assertTrue("adresseNullConstraint1 should be on db1", adresseNullConstraint1.getDb() == 1);
        Assert.assertTrue("adresseNullConstraint1 should have attribute NAME", adresseNullConstraint1.getAttributeName().equals("NAME"));
        Assert.assertTrue("adresseNullConstraint1 should have Type CheckNull", adresseNullConstraint1.getType() == ConstraintType.CheckNull);
        Assert.assertTrue("adresseNullConstraint2 should be on db2", adresseNullConstraint2.getDb() == 2);
        Assert.assertNull("adresseNullConstraint3 should be null", adresseNullConstraint3);

        // check horizontal on 3
        Constraint personNullConstraint1 = manager.getConstraint("PERSON", 1, "PERSON_NAME_NN");
        Constraint personNullConstraint2 = manager.getConstraint("PERSON", 2, "PERSON_NAME_NN");
        Constraint personNullConstraint3 = manager.getConstraint("PERSON", 3, "PERSON_NAME_NN");
        Assert.assertTrue("personNullConstraint1 should be on db1", personNullConstraint1.getDb() == 1);
        Assert.assertTrue("personNullConstraint1 should have attribute NAME", personNullConstraint1.getAttributeName().equals("NAME"));
        Assert.assertTrue("personNullConstraint1 should have type checkNull", personNullConstraint1.getType() == ConstraintType.CheckNull);
        Assert.assertTrue("personNullConstraint2 should be on db2", personNullConstraint2.getDb() == 2);
        Assert.assertTrue("personNullConstraint3 should be on db3", personNullConstraint3.getDb() == 3);
        
        // check vertical partition
        Constraint mitarbeiterNullConstraint = manager.getConstraint("MITARBEITER", 2, "MITARBEITER_NAME_NN");
        Assert.assertTrue("mitarbeiterNullConstraint should be on db2", mitarbeiterNullConstraint.getDb() == 2);
        Assert.assertTrue("mitarbeiterNullConstraint should have AttributeName NAME", mitarbeiterNullConstraint.getAttributeName().equals("NAME"));
        Assert.assertTrue("mitarbeiterNullConstraint should have Type CheckNull", mitarbeiterNullConstraint.getType() == ConstraintType.CheckNull);
        
        // check no partition
        Constraint checkNullConstraint = manager.getConstraint("BUCHUNG", 1, "BUCHUNG_MEILEN_NN");
        Assert.assertTrue("checkNullConstraint of Buchung should have MEILEN", checkNullConstraint.getAttributeName().equals("MEILEN"));
        Assert.assertTrue("checkNullConstraint of Buchung should have DbId 0", checkNullConstraint.getDb() == 1);
        Assert.assertTrue("checkNullConstraint of Buchung should have Name BUCHUNG_MEILEN_NN", checkNullConstraint.getName().equals("BUCHUNG_MEILEN_NN"));
        Assert.assertTrue("checkNullConstraint of Buchung should have TYPE CN", checkNullConstraint.getType() == ConstraintType.CheckNull);
    }
    
    /**
     * Methode zum Überprüfen der Between Constraints.
     */
    private void checkBetweenConstraints(){
        // check between constraints
        // check betweenConstraint vertical 
        BetweenConstraint betweenConstraintMitarbeiter = (BetweenConstraint) manager.getConstraint("MITARBEITER", 3, "MITARBEITER_LAND_BTW");
        Assert.assertTrue("betweenConstraintMitarbeiter should be on db3", betweenConstraintMitarbeiter.getDb() == 3);
        Assert.assertTrue("betweenConstraintMitarbeiter should be have type CheckBetween", betweenConstraintMitarbeiter.getType() == ConstraintType.CheckBetween);
        Assert.assertTrue("betweenConstraintMitarbeiter should have minType VARCHAR(100)", betweenConstraintMitarbeiter.getMinType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintMitarbeiter should have minValue A", betweenConstraintMitarbeiter.getMinValue().equals("A"));
        Assert.assertTrue("betweenConstraintMitarbeiter should have maxType VARCHAR(100)", betweenConstraintMitarbeiter.getMaxType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintMitarbeiter should have maxValue Z", betweenConstraintMitarbeiter.getMaxValue().equals("Z"));
        
        // check between on 2 horizontals
        BetweenConstraint betweenConstraintAdresse1 = (BetweenConstraint) manager.getConstraint("ADRESSE", 1, "ADRESSE_STATE_BTW");
        BetweenConstraint betweenConstraintAdresse2 = (BetweenConstraint) manager.getConstraint("ADRESSE", 2, "ADRESSE_STATE_BTW");
        BetweenConstraint betweenConstraintAdresse3 = (BetweenConstraint) manager.getConstraint("ADRESSE", 3, "ADRESSE_STATE_BTW");
        Assert.assertTrue("betweenConstraintAdresse1 should be on db1", betweenConstraintAdresse1.getDb() == 1);
        Assert.assertTrue("betweenConstraintAdresse1 should be have type CheckBetween", betweenConstraintAdresse1.getType() == ConstraintType.CheckBetween);
        Assert.assertTrue("betweenConstraintAdresse1 should have minType VARCHAR(1)", betweenConstraintAdresse1.getMinType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintAdresse1 should have minValue A", betweenConstraintAdresse1.getMinValue().equals("A"));
        Assert.assertTrue("betweenConstraintAdresse1 should have maxType VARCHAR(1)", betweenConstraintAdresse1.getMaxType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintAdresse1 should have maxValue Z", betweenConstraintAdresse1.getMaxValue().equals("Z"));
        Assert.assertTrue("betweenConstraintAdresse1 should have minType VARCHAR(1)", betweenConstraintAdresse1.getMinType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintAdresse1 should have minValue A", betweenConstraintAdresse1.getMinValue().equals("A"));
        Assert.assertTrue("betweenConstraintAdresse1 should have maxType VARCHAR(1)", betweenConstraintAdresse1.getMaxType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintAdresse1 should have maxValue Z", betweenConstraintAdresse1.getMaxValue().equals("Z"));
        Assert.assertTrue("betweenConstraintAdresse2 should be on db2", betweenConstraintAdresse2.getDb() == 2);
        Assert.assertNull("There should be no betweenConstraintAdresse3 on db3", betweenConstraintAdresse3);
        
        // check between on 3 horizontals
        BetweenConstraint betweenConstraintPerson1 = (BetweenConstraint) manager.getConstraint("PERSON", 1, "PERSON_STATE_BTW");
        BetweenConstraint betweenConstraintPerson2 = (BetweenConstraint) manager.getConstraint("PERSON", 2, "PERSON_STATE_BTW");
        BetweenConstraint betweenConstraintPerson3 = (BetweenConstraint) manager.getConstraint("PERSON", 3, "PERSON_STATE_BTW");
        Assert.assertTrue("betweenConstraintPerson1 should be on db1", betweenConstraintPerson1.getDb() == 1);
        Assert.assertTrue("betweenConstraintPerson1 should be have type CheckBetween", betweenConstraintPerson1.getType() == ConstraintType.CheckBetween);
        Assert.assertTrue("betweenConstraintPerson1 should have minType VARCHAR(1)", betweenConstraintPerson1.getMinType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintPerson1 should have minValue A", betweenConstraintPerson1.getMinValue().equals("A"));
        Assert.assertTrue("betweenConstraintPerson1 should have maxType VARCHAR(1)", betweenConstraintPerson1.getMaxType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintPerson1 should have maxValue Z", betweenConstraintPerson1.getMaxValue().equals("Z"));
        Assert.assertTrue("betweenConstraintPerson1 should have minType VARCHAR(1)", betweenConstraintPerson1.getMinType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintPerson1 should have minValue A", betweenConstraintPerson1.getMinValue().equals("A"));
        Assert.assertTrue("betweenConstraintPerson1 should have maxType VARCHAR(1)", betweenConstraintPerson1.getMaxType().equals("VARCHAR(1)"));
        Assert.assertTrue("betweenConstraintPerson1 should have maxValue Z", betweenConstraintPerson1.getMaxValue().equals("Z"));
        Assert.assertTrue("betweenConstraintPerson2 should be on db2", betweenConstraintPerson2.getDb() == 2);
        Assert.assertTrue("betweenConstraintPerson3 should be on db3", betweenConstraintPerson3.getDb() == 3);
        
        // check Between without partition
        BetweenConstraint checkBetweenConstraint = (BetweenConstraint) manager.getConstraint("BUCHUNG", 1, "BUCHUNG_MEILEN_BTW");

        Assert.assertTrue("checkBetweenConstraint of Buchung should have MEILEN", checkBetweenConstraint.getAttributeName().equals("MEILEN"));
        Assert.assertTrue("checkBetweenConstraint should be have type CheckBetween", checkBetweenConstraint.getType() == ConstraintType.CheckBetween);
        Assert.assertTrue("checkBetweenConstraint of Buchung should have DbId 1", checkBetweenConstraint.getDb() == 1);
        Assert.assertTrue("checkBetweenConstraint of Buchung should have NAME BUCHUNG_MEILEN_NN", checkBetweenConstraint.getName().equals("BUCHUNG_MEILEN_BTW"));
        Assert.assertTrue("checkBetweenConstraint of Buchung should have TYPE CBetween", checkBetweenConstraint.getType() == ConstraintType.CheckBetween);
        Assert.assertTrue("checkBetweenConstraint of Buchung should have MIN_VALUE 0", checkBetweenConstraint.getMinValue().equals("0"));
        Assert.assertTrue("checkBetweenConstraint of Buchung should have MIN_TYPE INTEGER", checkBetweenConstraint.getMinType().equals("INTEGER"));
        Assert.assertTrue("checkBetweenConstraint of Buchung should have MAX_VALUE 12", checkBetweenConstraint.getMaxValue().equals("12"));
        Assert.assertTrue("checkBetweenConstraint of Buchung should have MIN_TYPE INTEGER", checkBetweenConstraint.getMaxType().equals("INTEGER"));
    }
    
    /**
     * Methode zum Überprüfen der Binary Constraints
     */
    private void checkBinaryConstraints()
    {
        // check all binary cases
        // check attribute to constant cases
        // check vertical
        BinaryConstraint binaryConstraintMitarbeiter = (BinaryConstraint) manager.getConstraint("MITARBEITER", 3, "MITARBEITER_LAND_GQ");
        Assert.assertTrue("binaryConstraintMitarbeiter should be on db 3", binaryConstraintMitarbeiter.getDb() == 3);
        Assert.assertTrue("binaryConstraintMitarbeiter should have binary operator >", binaryConstraintMitarbeiter.getBinaryOperator().equals(">"));
        Assert.assertTrue("binaryConstraintMitarbeiter should have Operator greater", binaryConstraintMitarbeiter.getOperator() == ComparisonOperator.greater);
        Assert.assertTrue("binaryConstraintMitarbeiter should have type CheckComparison", binaryConstraintMitarbeiter.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("binaryConstraintMitarbeiter should have binary type a-c ", binaryConstraintMitarbeiter.getBinaryType() == BinaryType.attribute_constant);
        Assert.assertTrue("binaryConstraintMitarbeiter should have leftColumnName LAND", binaryConstraintMitarbeiter.getLeftColumnName().equals("LAND"));
        Assert.assertTrue("binaryConstraintMitarbeiter should have leftColumnType VARCHAR(100)", binaryConstraintMitarbeiter.getLeftColumnType().equals("VARCHAR(100)"));
        Assert.assertTrue("binaryConstraintMitarbeiter should have rightLiteralValue A", binaryConstraintMitarbeiter.getRightLiteralValue().equals("A"));
        Assert.assertTrue("binaryConstraintMitarbeiter should have rightLiteralType VARCHAR(100)", binaryConstraintMitarbeiter.getRightLiteralType().equals("VARCHAR(100)"));
        
        // check horizontal on 2
        BinaryConstraint binaryConstraintAdresse1 = (BinaryConstraint) manager.getConstraint("ADRESSE", 1, "ADRESSE_LAND_LQ");
        BinaryConstraint binaryConstraintAdresse2 = (BinaryConstraint) manager.getConstraint("ADRESSE", 2, "ADRESSE_LAND_LQ");
        BinaryConstraint binaryConstraintAdresse3 = (BinaryConstraint) manager.getConstraint("ADRESSE", 3, "ADRESSE_LAND_LQ");
        Assert.assertTrue("binaryConstraintAdresse1 should be on db 1", binaryConstraintAdresse1.getDb() == 1);
        Assert.assertTrue("binaryConstraintAdresse1 should have binary operator <", binaryConstraintAdresse1.getBinaryOperator().equals("<"));
        Assert.assertTrue("binaryConstraintAdresse1 should have Operator lower", binaryConstraintAdresse1.getOperator() == ComparisonOperator.lower);
        Assert.assertTrue("binaryConstraintAdresse1 should have type CheckComparison", binaryConstraintAdresse1.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("binaryConstraintAdresse1 should have binary type a-c ", binaryConstraintAdresse1.getBinaryType() == BinaryType.attribute_constant);
        Assert.assertTrue("binaryConstraintAdresse1 should have leftColumnName STATE", binaryConstraintAdresse1.getLeftColumnName().equals("STATE"));
        Assert.assertTrue("binaryConstraintAdresse1 should have leftColumnType VARCHAR(40)", binaryConstraintAdresse1.getLeftColumnType().equals("VARCHAR(40)"));
        Assert.assertTrue("binaryConstraintAdresse1 should have rightLiteralValue A", binaryConstraintAdresse1.getRightLiteralValue().equals("Z"));
        Assert.assertTrue("binaryConstraintAdresse1 should have rightLiteralType VARCHAR(40)", binaryConstraintAdresse1.getRightLiteralType().equals("VARCHAR(40)"));
        Assert.assertTrue("binaryConstraintAdresse2 should be on db 2", binaryConstraintAdresse2.getDb() == 2);
        Assert.assertNull("There should be no binaryConstraintAdresse3 on db3", binaryConstraintAdresse3);
        
        // check horizontal on 3
        BinaryConstraint binaryConstraintPerson1 = (BinaryConstraint) manager.getConstraint("PERSON", 1, "PERSON_LAND_GEQ");
        BinaryConstraint binaryConstraintPerson2 = (BinaryConstraint) manager.getConstraint("PERSON", 2, "PERSON_LAND_GEQ");
        BinaryConstraint binaryConstraintPerson3 = (BinaryConstraint) manager.getConstraint("PERSON", 3, "PERSON_LAND_GEQ");
        Assert.assertTrue("binaryConstraintPerson1 should be on db 1", binaryConstraintPerson1.getDb() == 1);
        Assert.assertTrue("binaryConstraintPerson1 should have binary operator <", binaryConstraintPerson1.getBinaryOperator().equals(">="));
        Assert.assertTrue("binaryConstraintPerson1 should have Operator lower", binaryConstraintPerson1.getOperator() == ComparisonOperator.greaterequal);
        Assert.assertTrue("binaryConstraintPerson1 should have type CheckComparison", binaryConstraintPerson1.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("binaryConstraintPerson1 should have binary type a-c ", binaryConstraintPerson1.getBinaryType() == BinaryType.attribute_constant);
        Assert.assertTrue("binaryConstraintPerson1 should have leftColumnName STATE", binaryConstraintPerson1.getLeftColumnName().equals("STATE"));
        Assert.assertTrue("binaryConstraintPerson1 should have leftColumnType VARCHAR(40)", binaryConstraintPerson1.getLeftColumnType().equals("VARCHAR(40)"));
        Assert.assertTrue("binaryConstraintPerson1 should have rightLiteralValue A", binaryConstraintPerson1.getRightLiteralValue().equals("A"));
        Assert.assertTrue("binaryConstraintPerson1 should have rightLiteralType VARCHAR(40)", binaryConstraintPerson1.getRightLiteralType().equals("VARCHAR(40)"));
        Assert.assertTrue("binaryConstraintAdresse2 should be on db 2", binaryConstraintPerson2.getDb() == 2);
        Assert.assertTrue("binaryConstraintAdresse2 should be on db 3", binaryConstraintPerson3.getDb() == 3);
        
        // check no partition
        BinaryConstraint binaryConstraintBuchung = (BinaryConstraint) manager.getConstraint("BUCHUNG", 1, "BUCHUNG_MEILEN_CHK");
        Assert.assertTrue("binaryConstraintBuchung should be on db 1", binaryConstraintBuchung.getDb() == 1);
        Assert.assertTrue("binaryConstraintBuchung should have binary operator <", binaryConstraintBuchung.getBinaryOperator().equals(">="));
        Assert.assertTrue("binaryConstraintBuchung should have Operator lower", binaryConstraintBuchung.getOperator() == ComparisonOperator.greaterequal);
        Assert.assertTrue("binaryConstraintBuchung should have type CheckComparison", binaryConstraintBuchung.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("binaryConstraintBuchung should have binary type a-c ", binaryConstraintBuchung.getBinaryType() == BinaryType.attribute_constant);
        Assert.assertTrue("binaryConstraintBuchung should have leftColumnName MEILEN", binaryConstraintBuchung.getLeftColumnName().equals("MEILEN"));
        Assert.assertTrue("binaryConstraintBuchung should have leftColumnType INTEGER", binaryConstraintBuchung.getLeftColumnType().equals("INTEGER"));
        Assert.assertTrue("binaryConstraintBuchung should have rightLiteralValue 0", binaryConstraintBuchung.getRightLiteralValue().equals("0"));
        Assert.assertTrue("binaryConstraintBuchung should have rightLiteralType INTEGER", binaryConstraintBuchung.getRightLiteralType().equals("INTEGER"));
        
        // check attribute to attribute cases
        // check vertical partition with different dbs
        BinaryConstraint binaryConstraintAttributeMitarbeiter = (BinaryConstraint) manager.getConstraint("MITARBEITER", 0, "MITARBEITER_LANDNAME_GQ");
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter of MITARBEITER should have DbId 0", binaryConstraintAttributeMitarbeiter.getDb() == 0);
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter of MITARBEITER should have TYPE Comparison", binaryConstraintAttributeMitarbeiter.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter of MITARBEITER should have Binary Type Attribute-Attribute", binaryConstraintAttributeMitarbeiter.getBinaryType() == BinaryType.attribute_attribute);
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter of MITARBEITER should have Operator GT", binaryConstraintAttributeMitarbeiter.getOperator() == ComparisonOperator.greater);
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter of MITARBEITER should have left column LAND", binaryConstraintAttributeMitarbeiter.getLeftColumnName().equals("LAND"));
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter of MITARBEITER should have left column type of VARCHAR(100)", binaryConstraintAttributeMitarbeiter.getLeftColumnType().equals("VARCHAR(100)"));
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter of MITARBEITER should have right column NAME", binaryConstraintAttributeMitarbeiter.getRightLiteralValue().equals("NAME"));
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter of MITARBEITER should have right column type of VARCHAR(50)", binaryConstraintAttributeMitarbeiter.getRightLiteralType().equals("VARCHAR(50)"));
        
        // check vertical partition with same dbs
        BinaryConstraint binaryConstraintAttributeMitarbeiter2 = (BinaryConstraint) manager.getConstraint("MITARBEITER", 3, "MITARBEITER_LANDCITY_GQ");
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter2 of MITARBEITER should have DbId 3", binaryConstraintAttributeMitarbeiter2.getDb() == 3);
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter2 of MITARBEITER should have TYPE Comparison", binaryConstraintAttributeMitarbeiter2.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter2 of MITARBEITER should have Binary Type Attribute-Attribute", binaryConstraintAttributeMitarbeiter2.getBinaryType() == BinaryType.attribute_attribute);
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter2 of MITARBEITER should have Operator GT", binaryConstraintAttributeMitarbeiter2.getOperator() == ComparisonOperator.greater);
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter2 of MITARBEITER should have left column LAND", binaryConstraintAttributeMitarbeiter2.getLeftColumnName().equals("LAND"));
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter2 of MITARBEITER should have left column type of VARCHAR(100)", binaryConstraintAttributeMitarbeiter2.getLeftColumnType().equals("VARCHAR(100)"));
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter2 of MITARBEITER should have right column NAME", binaryConstraintAttributeMitarbeiter2.getRightLiteralValue().equals("CITY"));
        Assert.assertTrue("binaryConstraintAttributeMitarbeiter2 of MITARBEITER should have right column type of VARCHAR(50)", binaryConstraintAttributeMitarbeiter2.getRightLiteralType().equals("VARCHAR(30)"));
        
        // check horizontal on 2
        BinaryConstraint binaryConstraintAttributeAdresse1 = (BinaryConstraint) manager.getConstraint("ADRESSE", 1, "ADRESSE_LANDNAME_LEQ");
        BinaryConstraint binaryConstraintAttributeAdresse2 = (BinaryConstraint) manager.getConstraint("ADRESSE", 2, "ADRESSE_LANDNAME_LEQ");
        BinaryConstraint binaryConstraintAttributeAdresse3 = (BinaryConstraint) manager.getConstraint("ADRESSE", 3, "ADRESSE_LANDNAME_LEQ");
        Assert.assertTrue("binaryConstraintAttributeAdresse1 should have DbId 1", binaryConstraintAttributeAdresse1.getDb() == 1);
        Assert.assertTrue("binaryConstraintAttributeAdresse1 should have TYPE Comparison", binaryConstraintAttributeAdresse1.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("binaryConstraintAttributeAdresse1 should have Binary Type Attribute-Attribute", binaryConstraintAttributeAdresse1.getBinaryType() == BinaryType.attribute_attribute);
        Assert.assertTrue("binaryConstraintAttributeAdresse1 should have Operator LET", binaryConstraintAttributeAdresse1.getOperator() == ComparisonOperator.lowerequal);
        Assert.assertTrue("binaryConstraintAttributeAdresse1 should have left column STATE", binaryConstraintAttributeAdresse1.getLeftColumnName().equals("STATE"));
        Assert.assertTrue("binaryConstraintAttributeAdresse1 should have left column type of VARCHAR(40)", binaryConstraintAttributeAdresse1.getLeftColumnType().equals("VARCHAR(40)"));
        Assert.assertTrue("binaryConstraintAttributeAdresse1 should have right column NAME", binaryConstraintAttributeAdresse1.getRightLiteralValue().equals("NAME"));
        Assert.assertTrue("binaryConstraintAttributeAdresse1 should have right column type of VARCHAR(50)", binaryConstraintAttributeAdresse1.getRightLiteralType().equals("VARCHAR(50)"));
        Assert.assertTrue("binaryConstraintAttributeAdresse2 should have DbId 2", binaryConstraintAttributeAdresse2.getDb() == 2);
        Assert.assertNull("binaryCOnstraintAttributeAdresse3 should be null", binaryConstraintAttributeAdresse3);

        // check horizontal on 3
        BinaryConstraint binaryConstraintAttributePerson1 = (BinaryConstraint) manager.getConstraint("PERSON", 1, "PERSON_LANDNAME_GEQ");
        BinaryConstraint binaryConstraintAttributePerson2 = (BinaryConstraint) manager.getConstraint("PERSON", 2, "PERSON_LANDNAME_GEQ");
        BinaryConstraint binaryConstraintAttributePerson3 = (BinaryConstraint) manager.getConstraint("PERSON", 3, "PERSON_LANDNAME_GEQ");
        Assert.assertTrue("binaryConstraintAttributePerson1 should have DbId 1", binaryConstraintAttributePerson1.getDb() == 1);
        Assert.assertTrue("binaryConstraintAttributePerson1 should have TYPE Comparison", binaryConstraintAttributePerson1.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("binaryConstraintAttributePerson1 should have Binary Type Attribute-Attribute", binaryConstraintAttributePerson1.getBinaryType() == BinaryType.attribute_attribute);
        Assert.assertTrue("binaryConstraintAttributePerson1 should have Operator GEQ", binaryConstraintAttributePerson1.getOperator() == ComparisonOperator.greaterequal);
        Assert.assertTrue("binaryConstraintAttributePerson1 should have left column STATE", binaryConstraintAttributePerson1.getLeftColumnName().equals("STATE"));
        Assert.assertTrue("binaryConstraintAttributePerson1 should have left column type of VARCHAR(40)", binaryConstraintAttributePerson1.getLeftColumnType().equals("VARCHAR(40)"));
        Assert.assertTrue("binaryConstraintAttributePerson1 should have right column NAME", binaryConstraintAttributePerson1.getRightLiteralValue().equals("NAME"));
        Assert.assertTrue("binaryConstraintAttributePerson1 should have right column type of VARCHAR(50)", binaryConstraintAttributePerson1.getRightLiteralType().equals("VARCHAR(50)"));
        Assert.assertTrue("binaryConstraintAttributePerson2 should have DbId 2", binaryConstraintAttributePerson2.getDb() == 2);
        Assert.assertTrue("binaryConstraintAttributePerson3 should have DbId 3", binaryConstraintAttributePerson3.getDb() == 3);

        // check no partition
        BinaryConstraint checkBinaryConstraint = (BinaryConstraint) manager.getConstraint("BUCHUNG", 1, "BUCHUNG_Binary");
        Assert.assertTrue("checkBinaryConstraint of Buchung should have DbId 1", checkBinaryConstraint.getDb() == 1);
        Assert.assertTrue("checkBinaryConstraint of Buchung should have Name BUCHUNG_Binary", checkBinaryConstraint.getName().equals("BUCHUNG_Binary"));
        Assert.assertTrue("checkBinaryConstraint of Buchung should have TYPE Comparison", checkBinaryConstraint.getType() == ConstraintType.CheckComparison);
        Assert.assertTrue("checkBinaryConstraint of Buchung should have Binary Type Attribute-Attribute", checkBinaryConstraint.getBinaryType() == BinaryType.attribute_attribute);
        Assert.assertTrue("checkBinaryConstraint of Buchung should have Operator GT", checkBinaryConstraint.getOperator() == ComparisonOperator.greater);
        Assert.assertTrue("checkBinaryConstraint of Buchung should have left column MEILEN", checkBinaryConstraint.getLeftColumnName().equals("MEILEN"));
        Assert.assertTrue("checkBinaryConstraint of Buchung should have left column type of INTEGER", checkBinaryConstraint.getLeftColumnType().equals("INTEGER"));
        Assert.assertTrue("checkBinaryConstraint of Buchung should have right column PREIS", checkBinaryConstraint.getRightLiteralValue().equals("PREIS"));
        Assert.assertTrue("checkBinaryConstraint of Buchung should have right column type of INTEGER", checkBinaryConstraint.getRightLiteralType().equals("INTEGER"));
    }
}
