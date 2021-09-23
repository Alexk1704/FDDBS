package junit.fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterSubTask;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.resolver.StatementResolverManager;
import fdbs.sql.semantic.SemanticValidator;
import fdbs.sql.unifier.ExecuteUpdateUnifierManager;
import fdbs.util.logger.Logger;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DDLStatementResolverTest {

    protected static FedConnection fedCon;
    protected static MetadataManager metadataManager;

    public DDLStatementResolverTest() {
    }

    @BeforeClass
    public static void setUpClass() throws FedException {
        fedCon = new FedConnection("VDBSB01", "VDBSB01");
        metadataManager = MetadataManager.getInstance(fedCon);
    }

    @AfterClass
    public static void tearDownClass() throws FedException {
        fedCon.close();
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("CREATE/DROP resolver tests started...");
        try {
            metadataManager.deleteMetadata();
            metadataManager.deleteTables();
            metadataManager.checkAndRefreshTables();
        } catch (FedException ex) {
        }
        Logger.infoln("Cleaning up tables...");
        try {
            executeStatement("drop table FLUGHAFEN cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFEN cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFEN cascade constraints", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGLINIE cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGLINIE cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGLINIE cascade constraints", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUG cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUG cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUG cascade constraints", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table PASSAGIER cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table PASSAGIER cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table BUCHUNG cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table BUCHUNG cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table BUCHUNG cascade constraints", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFEND cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENHTWO cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENHTWO cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENHTHREE cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENHTHREE cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENHTHREE cascade constraints", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENVTWO cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENVTWO cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENVTHREE cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENVTHREE cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHAFENVTHREE cascade constraints", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGD cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHTWO cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHTWO cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHTHREE cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHTHREE cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGHTHREE cascade constraints", 3);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGVTWO cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGVTWO cascade constraints", 2);
        } catch (FedException | SQLException ex) {

        }
        try {
            executeStatement("drop table FLUGVTHREE cascade constraints", 1);
        } catch (FedException | SQLException ex) {
        }

        try {
            executeStatement("drop table FLUGVTHREE cascade constraints", 2);
        } catch (FedException | SQLException ex) {
        }
        try {
            executeStatement("drop table FLUGVTHREE cascade constraints", 3);
        } catch (FedException | SQLException ex) {
        }

        Logger.infoln("Finished cleaning up tables...");
    }

    @After
    public void tearDown() {
        Logger.infoln("CREATE statement tests finished...");
    }

    @Test
    public void testStatements() throws FedException {
        AST ast = null;
        // PEINL TEST CASES - CREPARTABS.SQL
        ast = parseStatement("create table FLUGHAFEN (\n"
                + "FHC		varchar(3),\n"
                + "LAND		varchar(3),\n"
                + "STADT		varchar(50),\n"
                + "NAME		varchar(50),\n"
                + "constraint FLUGHAFEN_PS primary key (FHC)\n"
                + ")\n"
                + "VERTICAL ((FHC, LAND), (STADT, NAME))");
        SQLExecuterTask executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("FHC VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("LAND VARCHAR(3)"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("STADT VARCHAR(50)"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("NAME VARCHAR(50)"));

        Assert.assertTrue(getDb2Query(executerTask).contains("FHC VARCHAR(3)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("STADT VARCHAR(50)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("NAME VARCHAR(50)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("LAND VARCHAR(3)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHAFEN_PS PRIMARY KEY (FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHAFEN_PS PRIMARY KEY (FHC)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGLINIE (\n"
                + "FLC		varchar(2),\n"
                + "LAND		varchar(3),\n"
                + "HUB		varchar(3),\n"
                + "NAME                 varchar(30),\n"
                + "ALLIANZ	varchar(20),\n"
                + "constraint FLUGLINIE_PS primary key (FLC),\n"
                + "constraint FLUGLINIE_FS_HUB foreign key (HUB) references FLUGHAFEN(FHC),\n"
                + "constraint FLUGLINIE_LAND_NN check (LAND is not null),\n"
                + "constraint FLUGLINIE_ALLIANZ_CHK check (ALLIANZ != 'BlackList')\n"
                + ")\n"
                + "HORIZONTAL (FLC('KK','MM'))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("LAND VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("HUB VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("NAME VARCHAR(30)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("ALLIANZ VARCHAR(20)"));

        Assert.assertTrue(getDb2Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("LAND VARCHAR(3)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("HUB VARCHAR(3)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("NAME VARCHAR(30)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("ALLIANZ VARCHAR(20)"));

        Assert.assertTrue(getDb3Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("LAND VARCHAR(3)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("HUB VARCHAR(3)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("NAME VARCHAR(30)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("ALLIANZ VARCHAR(20)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGLINIE_PS PRIMARY KEY (FLC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGLINIE_PS PRIMARY KEY (FLC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGLINIE_PS PRIMARY KEY (FLC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGLINIE_FS_HUB FOREIGN KEY (HUB) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGLINIE_FS_HUB FOREIGN KEY (HUB) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGLINIE_FS_HUB FOREIGN KEY (HUB) REFERENCES FLUGHAFEN(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGLINIE_LAND_NN CHECK (LAND IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGLINIE_LAND_NN CHECK (LAND IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGLINIE_LAND_NN CHECK (LAND IS NOT NULL)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGLINIE_ALLIANZ_CHK CHECK (ALLIANZ != 'BlackList')"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGLINIE_ALLIANZ_CHK CHECK (ALLIANZ != 'BlackList')"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGLINIE_ALLIANZ_CHK CHECK (ALLIANZ != 'BlackList')"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUG (\n"
                + "FNR        integer,\n"
                + "FLC        varchar(2),\n"
                + "FLNR       integer,\n"
                + "VON        varchar(3),\n"
                + "NACH       varchar(3),\n"
                + "AB             integer,\n"
                + "AN         integer,\n"
                + "constraint FLUG_PS primary key (FNR),\n"
                + "constraint FLUG_FS_FLC foreign key (FLC) references FLUGLINIE(FLC),\n"
                + "constraint FLUG_FS_VON foreign key (VON) references FLUGHAFEN(FHC),\n"
                + "constraint FLUG_FS_NACH foreign key (NACH) references FLUGHAFEN(FHC),\n"
                + "constraint FLUG_VON_NN check (VON is not null),\n"
                + "constraint FLUG_NACH_NN check (NACH is not null),\n"
                + "constraint FLUG_AB_NN check (AB is not null),\n"
                + "constraint FLUG_AN_NN check (AN is not null),\n"
                + "constraint FLUG_AB_CHK check (AB between 0 and 2400),\n"
                + "constraint FLUG_AN_CHK check (AN between 0 and 2400),\n"
                + "constraint FLUG_VONNACH_CHK check (VON != NACH)\n"
                + ")\n"
                + "HORIZONTAL (FLC('KK','MM'))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("FNR INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("FLNR INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("VON VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("NACH VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("AB INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("AN INTEGER"));

        Assert.assertTrue(getDb2Query(executerTask).contains("FNR INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("FLNR INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("VON VARCHAR(3)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("NACH VARCHAR(3)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("AB INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("AN INTEGER"));

        Assert.assertTrue(getDb3Query(executerTask).contains("FNR INTEGER"));
        Assert.assertTrue(getDb3Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("FLNR INTEGER"));
        Assert.assertTrue(getDb3Query(executerTask).contains("VON VARCHAR(3)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("NACH VARCHAR(3)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("AB INTEGER"));
        Assert.assertTrue(getDb3Query(executerTask).contains("AN INTEGER"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_PS PRIMARY KEY (FNR)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_PS PRIMARY KEY (FNR)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_PS PRIMARY KEY (FNR)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_FS_FLC FOREIGN KEY (FLC) REFERENCES FLUGLINIE(FLC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_FS_FLC FOREIGN KEY (FLC) REFERENCES FLUGLINIE(FLC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_FS_FLC FOREIGN KEY (FLC) REFERENCES FLUGLINIE(FLC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_FS_VON FOREIGN KEY (VON) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_FS_VON FOREIGN KEY (VON) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUG_FS_VON FOREIGN KEY (VON) REFERENCES FLUGHAFEN(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_FS_NACH FOREIGN KEY (NACH) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_FS_NACH FOREIGN KEY (NACH) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUG_FS_NACH FOREIGN KEY (NACH) REFERENCES FLUGHAFEN(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_VON_NN CHECK (VON IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_VON_NN CHECK (VON IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_VON_NN CHECK (VON IS NOT NULL)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_NACH_NN CHECK (NACH IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_NACH_NN CHECK (NACH IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_NACH_NN CHECK (NACH IS NOT NULL)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_AB_NN CHECK (AB IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_AB_NN CHECK (AB IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_AB_NN CHECK (AB IS NOT NULL)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_AN_NN CHECK (AN IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_AN_NN CHECK (AN IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_AN_NN CHECK (AN IS NOT NULL)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_AB_CHK CHECK (AB BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_AB_CHK CHECK (AB BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_AB_CHK CHECK (AB BETWEEN 0 AND 2400)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_AN_CHK CHECK (AN BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_AN_CHK CHECK (AN BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_AN_CHK CHECK (AN BETWEEN 0 AND 2400)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUG_VONNACH_CHK CHECK (VON != NACH)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUG_VONNACH_CHK CHECK (VON != NACH"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUG_VONNACH_CHK CHECK (VON != NACH)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table PASSAGIER (\n"
                + "PNR		integer,\n"
                + "NAME                         varchar(40),\n"
                + "VORNAME                  varchar(40),\n"
                + "LAND		varchar(3),\n"
                + "constraint PASSAGIER_PS primary key (PNR),\n"
                + "constraint PASSAGIER_NAME_NN check (NAME is not null)\n"
                + ")\n"
                + "VERTICAL ((PNR, NAME),(VORNAME, LAND))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("PNR INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("NAME VARCHAR(40)"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("VORNAME VARCHAR(40)"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("LAND VARCHAR(3)"));

        Assert.assertTrue(getDb2Query(executerTask).contains("PNR INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("VORNAME VARCHAR(40)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("LAND VARCHAR(3)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT PASSAGIER_PS PRIMARY KEY (PNR)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT PASSAGIER_PS PRIMARY KEY (PNR)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT PASSAGIER_NAME_NN CHECK (NAME IS NOT NULL)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT PASSAGIER_NAME_NN CHECK (NAME IS NOT NULL)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table BUCHUNG (\n"
                + "BNR             integer,\n"
                + "PNR            integer,\n"
                + "FLC            varchar(2),\n"
                + "FLNR           integer,\n"
                + "VON            varchar(3),\n"
                + "NACH           varchar(3),\n"
                + "TAG               varchar(20),\n"
                + "MEILEN             integer,\n"
                + "PREIS               integer,\n"
                + "constraint BUCHUNG_PS primary key (BNR),\n"
                + "constraint BUCHUNG_FS_PNR foreign key (PNR) references PASSAGIER(PNR),\n"
                + "constraint BUCHUNG_FS_FLC foreign key (FLC) references FLUGLINIE(FLC),\n"
                + "constraint BUCHUNG_FS_VON foreign key (VON) references FLUGHAFEN(FHC),\n"
                + "constraint BUCHUNG_FS_NACH foreign key (NACH) references FLUGHAFEN(FHC),\n"
                + "constraint BUCHUNG_NACH_NN check (NACH is not null),\n"
                + "constraint BUCHUNG_MEILEN_NN check (MEILEN is not null),\n"
                + "constraint BUCHUNG_PREIS_NN check (PREIS is not null),\n"
                + "constraint BUCHUNG_MEILEN_CHK check (MEILEN >= 0),\n"
                + "constraint BUCHUNG_PREIS_CHK check (PREIS > 0)\n"
                + ")\n"
                + "HORIZONTAL (PNR(35,70))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("BNR INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("PNR INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("FLNR INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("VON VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("NACH VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("TAG VARCHAR(20)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("MEILEN INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("PREIS INTEGER"));

        Assert.assertTrue(getDb2Query(executerTask).contains("BNR INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("PNR INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("FLNR INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("VON VARCHAR(3)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("NACH VARCHAR(3)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("TAG VARCHAR(20)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("MEILEN INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("PREIS INTEGER"));

        Assert.assertTrue(getDb3Query(executerTask).contains("BNR INTEGER"));
        Assert.assertTrue(getDb3Query(executerTask).contains("PNR INTEGER"));
        Assert.assertTrue(getDb3Query(executerTask).contains("FLC VARCHAR(2)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("FLNR INTEGER"));
        Assert.assertTrue(getDb3Query(executerTask).contains("VON VARCHAR(3)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("NACH VARCHAR(3)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("TAG VARCHAR(20)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("MEILEN INTEGER"));
        Assert.assertTrue(getDb3Query(executerTask).contains("PREIS INTEGER"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_PS PRIMARY KEY (BNR)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_PS PRIMARY KEY (BNR)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_PS PRIMARY KEY (BNR)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_PNR FOREIGN KEY (PNR) REFERENCES PASSAGIER(PNR)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_PNR FOREIGN KEY (PNR) REFERENCES PASSAGIER(PNR)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_PNR FOREIGN KEY (PNR) REFERENCES PASSAGIER(PNR)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_FLC FOREIGN KEY (FLC) REFERENCES FLUGLINIE(FLC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_FLC FOREIGN KEY (FLC) REFERENCES FLUGLINIE(FLC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_FLC FOREIGN KEY (FLC) REFERENCES FLUGLINIE(FLC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_VON FOREIGN KEY (VON) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_VON FOREIGN KEY (VON) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_VON FOREIGN KEY (VON) REFERENCES FLUGHAFEN(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_NACH FOREIGN KEY (NACH) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_NACH FOREIGN KEY (NACH) REFERENCES FLUGHAFEN(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_FS_NACH FOREIGN KEY (NACH) REFERENCES FLUGHAFEN(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_NACH_NN CHECK (NACH IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_NACH_NN CHECK (NACH IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_NACH_NN CHECK (NACH IS NOT NULL)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_MEILEN_NN CHECK (MEILEN IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_MEILEN_NN CHECK (MEILEN IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_MEILEN_NN CHECK (MEILEN IS NOT NULL)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_PREIS_NN CHECK (PREIS IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_PREIS_NN CHECK (PREIS IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_PREIS_NN CHECK (PREIS IS NOT NULL)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_MEILEN_CHK CHECK (MEILEN >= 0)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_MEILEN_CHK CHECK (MEILEN >= 0"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_MEILEN_CHK CHECK (MEILEN >= 0)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT BUCHUNG_PREIS_CHK CHECK (PREIS > 0)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT BUCHUNG_PREIS_CHK CHECK (PREIS > 0"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT BUCHUNG_PREIS_CHK CHECK (PREIS > 0)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // CUSTOM TEST CASES
        // ATTRIBUTE & CONSTRAINT DISTRIBUTION FOR D, V2, V3, H2, H3
        // DEFAULT
        ast = parseStatement("create table FLUGHAFEND (\n"
                + "FHC		varchar(3),\n"
                + "LAND		varchar(3),\n"
                + "STADT		varchar(50),\n"
                + "NAME		varchar(50) DEFAULT ('peter'),\n"
                + "AINT                                integer,\n"
                + "BINT                                integer,\n"
                + "CINT                                integer DEFAULT (1337),\n"
                + "DINT                                integer,\n"
                + "constraint FHC_PK primary key (FHC),\n"
                + "constraint STADT_UNIQUE unique (STADT),\n "
                + "constraint NAME_NN check (NAME is not null),\n"
                + "constraint CHKGR_AINT check (AINT > 0),\n"
                + "constraint CHKLES_BINT check (BINT < 5000),\n"
                + "constraint CHKLESEQ_BINT check (BINT <= 5000),\n"
                + "constraint CHKGREQ_CINT check (CINT >= 3000),\n"
                + "constraint CHKEQ_DINT check (DINT = 1337),\n"
                + "constraint CHKNEQ_CINT check (CINT != 3337),\n"
                + "constraint CHKBTW_AINT check (AINT between 0 and 2400),\n"
                + "constraint CHKBTW_CINT check (CINT between 3333 and 9999),\n"
                + "constraint CHK_LANDSTADT check (LAND != STADT),\n"
                + "constraint CHKNEQ_NAME check (NAME != 'BlackList'),\n"
                + "constraint CHKEQ_LAND check (LAND = 'GER')\n"
                + ")");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb3SubTasks(executerTask).isEmpty());

        // ATTRIBUTES
        Assert.assertTrue(getDb1Query(executerTask).contains("FHC VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("LAND VARCHAR(3)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("STADT VARCHAR(50)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("NAME VARCHAR(50)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("AINT INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("BINT INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CINT INTEGER"));
        Assert.assertTrue(getDb1Query(executerTask).contains("DINT INTEGER"));
        // ATTRIBUTE ORDER + DEFAULT
        Assert.assertTrue(getDb1Query(executerTask).contains("FHC VARCHAR(3), LAND VARCHAR(3), STADT VARCHAR(50), NAME VARCHAR(50) DEFAULT 'peter', AINT INTEGER, BINT INTEGER, CINT INTEGER DEFAULT 1337, DINT INTEGER"));
        // PK CONSTRAINT
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FHC_PK PRIMARY KEY (FHC)"));
        // UNIQUE CONSTRAINT
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT STADT_UNIQUE UNIQUE (STADT)"));
        // NOT NULL CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT NAME_NN CHECK (NAME IS NOT NULL)"));
        // BETWEEN CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (INTEGER)
        // GREATER, LESSER
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKLES_BINT CHECK (BINT < 5000"));
        // GREATER EQUALS, LESSER EQUALS
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKLESEQ_BINT CHECK (BINT <= 5000"));
        // EQUALS, NOT EQUALS
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKNEQ_CINT CHECK (CINT != 3337"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (VARCHAR)
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT CHKEQ_LAND CHECK (LAND = 'GER'"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // HORIZONTAL ON TWO PARTITIONS SPLIT VIA VARCHAR
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGHAFENHTWO (\n"
                + "FHC		varchar(3),\n"
                + "LAND		varchar(3),\n"
                + "STADT		varchar(50),\n"
                + "NAME		varchar(50),\n"
                + "AINT                                integer,\n"
                + "BINT                                integer,\n"
                + "CINT                                integer,\n"
                + "DINT                                integer,\n"
                + "constraint HTWOFHC_PK primary key (FHC),\n"
                + "constraint HTWOSTADT_UNIQUE unique (STADT),\n "
                + "constraint HTWONAME_NN check (NAME is not null),\n"
                + "constraint HTWOCHKGR_AINT check (AINT > 0),\n"
                + "constraint HTWOCHKLES_BINT check (BINT < 5000),\n"
                + "constraint HTWOCHKLESEQ_BINT check (BINT <= 5000),\n"
                + "constraint HTWOCHKGREQ_CINT check (CINT >= 3000),\n"
                + "constraint HTWOCHKEQ_DINT check (DINT = 1337),\n"
                + "constraint HTWOCHKNEQ_CINT check (CINT != 3337),\n"
                + "constraint HTWOCHKBTW_AINT check (AINT between 0 and 2400),\n"
                + "constraint HTWOCHKBTW_CINT check (CINT between 3333 and 9999),\n"
                + "constraint HTWOCHK_LANDSTADT check (LAND != STADT),\n"
                + "constraint HTWOCHKNEQ_NAME check (NAME != 'BlackList'),\n"
                + "constraint HTWOCHKEQ_LAND check (LAND = 'GER')\n"
                + ")"
                + "HORIZONTAL (FHC('DD'))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb3SubTasks(executerTask).isEmpty());

        // ATTRIBUTE ORDER
        Assert.assertTrue(getDb1Query(executerTask).contains("FHC VARCHAR(3), LAND VARCHAR(3), STADT VARCHAR(50), NAME VARCHAR(50), AINT INTEGER, BINT INTEGER, CINT INTEGER, DINT INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("FHC VARCHAR(3), LAND VARCHAR(3), STADT VARCHAR(50), NAME VARCHAR(50), AINT INTEGER, BINT INTEGER, CINT INTEGER, DINT INTEGER"));
        // PK CONSTRAINT
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOFHC_PK PRIMARY KEY (FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOFHC_PK PRIMARY KEY (FHC)"));
        // UNIQUE CONSTRAINT
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOSTADT_UNIQUE UNIQUE (STADT)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOSTADT_UNIQUE UNIQUE (STADT)"));
        // NOT NULL CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWONAME_NN CHECK (NAME IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWONAME_NN CHECK (NAME IS NOT NULL)"));
        // BETWEEN CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (INTEGER)
        // GREATER, LESSER
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKLES_BINT CHECK (BINT < 5000"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKLES_BINT CHECK (BINT < 5000"));
        // GREATER EQUALS, LESSER EQUALS
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKLESEQ_BINT CHECK (BINT <= 5000"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKLESEQ_BINT CHECK (BINT <= 5000"));
        // EQUALS, NOT EQUALS
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKNEQ_CINT CHECK (CINT != 3337"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKNEQ_CINT CHECK (CINT != 3337"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (VARCHAR)
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTWOCHKEQ_LAND CHECK (LAND = 'GER'"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTWOCHKEQ_LAND CHECK (LAND = 'GER'"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // HORIZONTAL ON THREE PARTITIONS SPLIT VIA INTEGER
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGHAFENHTHREE (\n"
                + "FHC		varchar(3),\n"
                + "LAND		varchar(3),\n"
                + "STADT		varchar(50),\n"
                + "NAME		varchar(50),\n"
                + "AINT                                integer,\n"
                + "BINT                                integer,\n"
                + "CINT                                integer,\n"
                + "DINT                                integer,\n"
                + "constraint HTHREEFHC_PK primary key (FHC),\n"
                + "constraint HTHREESTADT_UNIQUE unique (STADT),\n "
                + "constraint HTHREENAME_NN check (NAME is not null),\n"
                + "constraint HTHREECHKGR_AINT check (AINT > 0),\n"
                + "constraint HTHREECHKLES_BINT check (BINT < 5000),\n"
                + "constraint HTHREECHKLESEQ_BINT check (BINT <= 5000),\n"
                + "constraint HTHREECHKGREQ_CINT check (CINT >= 3000),\n"
                + "constraint HTHREECHKEQ_DINT check (DINT = 1337),\n"
                + "constraint HTHREECHKNEQ_CINT check (CINT != 3337),\n"
                + "constraint HTHREECHKBTW_AINT check (AINT between 0 and 2400),\n"
                + "constraint HTHREECHKBTW_CINT check (CINT between 3333 and 9999),\n"
                + "constraint HTHREECHK_LANDSTADT check (LAND != STADT),\n"
                + "constraint HTHREECHKNEQ_NAME check (NAME != 'BlackList'),\n"
                + "constraint HTHREECHKEQ_LAND check (LAND = 'GER')\n"
                + ")"
                + "HORIZONTAL (AINT(50,5000))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb3SubTasks(executerTask).isEmpty());

        // ATTRIBUTE ORDER
        Assert.assertTrue(getDb1Query(executerTask).contains("FHC VARCHAR(3), LAND VARCHAR(3), STADT VARCHAR(50), NAME VARCHAR(50), AINT INTEGER, BINT INTEGER, CINT INTEGER, DINT INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("FHC VARCHAR(3), LAND VARCHAR(3), STADT VARCHAR(50), NAME VARCHAR(50), AINT INTEGER, BINT INTEGER, CINT INTEGER, DINT INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("FHC VARCHAR(3), LAND VARCHAR(3), STADT VARCHAR(50), NAME VARCHAR(50), AINT INTEGER, BINT INTEGER, CINT INTEGER, DINT INTEGER"));
        // PK CONSTRAINT
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREEFHC_PK PRIMARY KEY (FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREEFHC_PK PRIMARY KEY (FHC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREEFHC_PK PRIMARY KEY (FHC)"));
        // UNIQUE CONSTRAINT
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREESTADT_UNIQUE UNIQUE (STADT)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREESTADT_UNIQUE UNIQUE (STADT)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREESTADT_UNIQUE UNIQUE (STADT)"));
        // NOT NULL CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREENAME_NN CHECK (NAME IS NOT NULL)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREENAME_NN CHECK (NAME IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREENAME_NN CHECK (NAME IS NOT NULL)"));
        // BETWEEN CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (INTEGER)
        // GREATER, LESSER
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKLES_BINT CHECK (BINT < 5000"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKLES_BINT CHECK (BINT < 5000"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKLES_BINT CHECK (BINT < 5000"));
        // GREATER EQUALS, LESSER EQUALS
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKLESEQ_BINT CHECK (BINT <= 5000"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKLESEQ_BINT CHECK (BINT <= 5000"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKLESEQ_BINT CHECK (BINT <= 5000"));
        // EQUALS, NOT EQUALS
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKNEQ_CINT CHECK (CINT != 3337"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKNEQ_CINT CHECK (CINT != 3337"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKNEQ_CINT CHECK (CINT != 3337"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (VARCHAR)
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT HTHREECHKEQ_LAND CHECK (LAND = 'GER'"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT HTHREECHKEQ_LAND CHECK (LAND = 'GER'"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT HTHREECHKEQ_LAND CHECK (LAND = 'GER'"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // VERTICAL ON TWO PARTITIONS
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGHAFENVTWO (\n"
                + "FHC		varchar(3),\n"
                + "LAND		varchar(3),\n"
                + "STADT		varchar(50),\n"
                + "NAME		varchar(50),\n"
                + "AINT                                integer,\n"
                + "BINT                                integer,\n"
                + "CINT                                integer,\n"
                + "DINT                                integer,\n"
                + "constraint VTWOFHC_PK primary key (FHC),\n"
                + "constraint VTWOSTADT_UNIQUE unique (STADT),\n "
                + "constraint VTWONAME_NN check (NAME is not null),\n"
                + "constraint VTWOCHKGR_AINT check (AINT > 0),\n"
                + "constraint VTWOCHKLES_BINT check (BINT < 5000),\n"
                + "constraint VTWOCHKLESEQ_BINT check (BINT <= 5000),\n"
                + "constraint VTWOCHKGREQ_CINT check (CINT >= 3000),\n"
                + "constraint VTWOCHKEQ_DINT check (DINT = 1337),\n"
                + "constraint VTWOCHKNEQ_CINT check (CINT != 3337),\n"
                + "constraint VTWOCHKBTW_AINT check (AINT between 0 and 2400),\n"
                + "constraint VTWOCHKBTW_CINT check (CINT between 3333 and 9999),\n"
                + "constraint VTWOCHK_LANDSTADT check (LAND != STADT),\n"
                + "constraint VTWOCHKNEQ_NAME check (NAME != 'BlackList'),\n"
                + "constraint VTWOCHKEQ_LAND check (LAND = 'GER')\n"
                + ")"
                + "VERTICAL ((CINT, FHC, AINT, NAME),(DINT, LAND, BINT, STADT))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb3SubTasks(executerTask).isEmpty());

        // ATTRIBUTE ORDER
        Assert.assertTrue(getDb1Query(executerTask).contains("CINT INTEGER, FHC VARCHAR(3), AINT INTEGER, NAME VARCHAR(50)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("FHC VARCHAR(3), DINT INTEGER, LAND VARCHAR(3), BINT INTEGER, STADT VARCHAR(50)"));
        // PK CONSTRAINT
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTWOFHC_PK PRIMARY KEY (FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTWOFHC_PK PRIMARY KEY (FHC)"));
        // UNIQUE CONSTRAINT
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTWOSTADT_UNIQUE UNIQUE (STADT)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTWOSTADT_UNIQUE UNIQUE (STADT)"));
        // NOT NULL CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTWONAME_NN CHECK (NAME IS NOT NULL)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTWONAME_NN CHECK (NAME IS NOT NULL)"));
        // BETWEEN CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (INTEGER)
        // GREATER, LESSER
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKLES_BINT CHECK (BINT < 5000"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKLES_BINT CHECK (BINT < 5000"));
        // GREATER EQUALS, LESSER EQUALS
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKLESEQ_BINT CHECK (BINT <= 5000"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKLESEQ_BINT CHECK (BINT <= 5000"));
        // EQUALS, NOT EQUALS
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKNEQ_CINT CHECK (CINT != 3337"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKNEQ_CINT CHECK (CINT != 3337"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (VARCHAR)
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTWOCHKEQ_LAND CHECK (LAND = 'GER'"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTWOCHKEQ_LAND CHECK (LAND = 'GER'"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // VERTICAL ON THREE PARTITIONS
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGHAFENVTHREE (\n"
                + "FHC		varchar(3),\n"
                + "LAND		varchar(3),\n"
                + "STADT		varchar(50),\n"
                + "NAME		varchar(50),\n"
                + "AINT                                integer,\n"
                + "BINT                                integer,\n"
                + "CINT                                integer,\n"
                + "DINT                                integer,\n"
                + "constraint VTHREEFHC_PK primary key (FHC),\n"
                + "constraint VTHREESTADT_UNIQUE unique (STADT),\n "
                + "constraint VTHREENAME_NN check (NAME is not null),\n"
                + "constraint VTHREECHKGR_AINT check (AINT > 0),\n"
                + "constraint VTHREECHKLES_BINT check (BINT < 5000),\n"
                + "constraint VTHREECHKLESEQ_BINT check (BINT <= 5000),\n"
                + "constraint VTHREECHKGREQ_CINT check (CINT >= 3000),\n"
                + "constraint VTHREECHKEQ_DINT check (DINT = 1337),\n"
                + "constraint VTHREECHKNEQ_CINT check (CINT != 3337),\n"
                + "constraint VTHREECHKBTW_AINT check (AINT between 0 and 2400),\n"
                + "constraint VTHREECHKBTW_CINT check (CINT between 3333 and 9999),\n"
                + "constraint VTHREECHK_LANDSTADT check (LAND != STADT),\n"
                + "constraint VTHREECHKNEQ_NAME check (NAME != 'BlackList'),\n"
                + "constraint VTHREECHKEQ_LAND check (LAND = 'GER')\n"
                + ")"
                + "VERTICAL ((CINT, AINT),(DINT, LAND, FHC),(NAME, BINT, STADT))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb3SubTasks(executerTask).isEmpty());

        // ATTRIBUTE ORDER
        Assert.assertTrue(getDb1Query(executerTask).contains("FHC VARCHAR(3), CINT INTEGER, AINT INTEGER"));
        Assert.assertTrue(getDb2Query(executerTask).contains("DINT INTEGER, LAND VARCHAR(3), FHC VARCHAR(3)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("FHC VARCHAR(3), NAME VARCHAR(50), BINT INTEGER, STADT VARCHAR(50)"));
        // PK CONSTRAINT
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTHREEFHC_PK PRIMARY KEY (FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTHREEFHC_PK PRIMARY KEY (FHC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT VTHREEFHC_PK PRIMARY KEY (FHC)"));
        // UNIQUE CONSTRAINT
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTHREESTADT_UNIQUE UNIQUE (STADT)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREESTADT_UNIQUE UNIQUE (STADT)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT VTHREESTADT_UNIQUE UNIQUE (STADT)"));
        // NOT NULL CHECK
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTHREENAME_NN CHECK (NAME IS NOT NULL)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREENAME_NN CHECK (NAME IS NOT NULL)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT VTHREENAME_NN CHECK (NAME IS NOT NULL)"));
        // BETWEEN CHECK
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKBTW_AINT CHECK (AINT BETWEEN 0 AND 2400)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKBTW_CINT CHECK (CINT BETWEEN 3333 AND 9999)"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (NTEGER)
        // GREATER, LESSER
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKGR_AINT CHECK (AINT > 0"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKLES_BINT CHECK (BINT < 5000"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKLES_BINT CHECK (BINT < 5000"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKLES_BINT CHECK (BINT < 5000"));
        // GREATER EQUALS, LESSER EQUALS
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKGREQ_CINT CHECK (CINT >= 3000"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKLESEQ_BINT CHECK (BINT <= 5000"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKLESEQ_BINT CHECK (BINT <= 5000"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKLESEQ_BINT CHECK (BINT <= 5000"));
        // EQUALS, NOT EQUALS
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKEQ_DINT CHECK (DINT = 1337"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKNEQ_CINT CHECK (CINT != 3337"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKNEQ_CINT CHECK (CINT != 3337"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKNEQ_CINT CHECK (CINT != 3337"));
        // COMPARISON CHECK: ATTRIBUTE - CONSTANT (VARCHAR)
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKNEQ_NAME CHECK (NAME != 'BlackList'"));
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT VTHREECHKEQ_LAND CHECK (LAND = 'GER'"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT VTHREECHKEQ_LAND CHECK (LAND = 'GER'"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT VTHREECHKEQ_LAND CHECK (LAND = 'GER'"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // FOREIGN KEY CHECKS
        // DEFAULT TO DEFAULT,VTWO,VTHREE,HTWO,HTHREE
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGD (\n"
                + "FNR        integer,\n"
                + "FLC        varchar(2),\n"
                + "FLNR       integer,\n"
                + "VON        varchar(3),\n"
                + "NACH       varchar(3),\n"
                + "AB             integer,\n"
                + "AN         integer,\n"
                + "constraint FLUGD_PS primary key (FNR),\n"
                + "constraint FLUGD_FS_VOND foreign key (VON) references FLUGHAFEND(FHC),\n"
                + "constraint FLUGD_FS_VONHTWO foreign key (VON) references FLUGHAFENHTWO(FHC),\n"
                + "constraint FLUGD_FS_VONHTHREE foreign key (VON) references FLUGHAFENHTHREE(FHC),\n"
                + "constraint FLUGD_FS_VONVTWO foreign key (VON) references FLUGHAFENVTWO(FHC),\n"
                + "constraint FLUGD_FS_VONVTHREE foreign key (VON) references FLUGHAFENVTHREE(FHC)\n"
                + ")");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGD_PS PRIMARY KEY (FNR)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGD_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGD_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGD_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGD_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGD_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // HTWO TO DEFAULT,VTWO,VTHREE,HTWO,HTHREE
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGHTWO (\n"
                + "FNR        integer,\n"
                + "FLC        varchar(2),\n"
                + "FLNR       integer,\n"
                + "VON        varchar(3),\n"
                + "NACH       varchar(3),\n"
                + "AB             integer,\n"
                + "AN         integer,\n"
                + "constraint FLUGHTWO_PS primary key (FNR),\n"
                + "constraint FLUGHTWO_FS_VOND foreign key (VON) references FLUGHAFEND(FHC),\n"
                + "constraint FLUGHTWO_FS_VONHTWO foreign key (VON) references FLUGHAFENHTWO(FHC),\n"
                + "constraint FLUGHTWO_FS_VONHTHREE foreign key (VON) references FLUGHAFENHTHREE(FHC),\n"
                + "constraint FLUGHTWO_FS_VONVTWO foreign key (VON) references FLUGHAFENVTWO(FHC),\n"
                + "constraint FLUGHTWO_FS_VONVTHREE foreign key (VON) references FLUGHAFENVTHREE(FHC)\n"
                + ")\n"
                + "HORIZONTAL (FLC('KK'))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTWO_PS PRIMARY KEY (FNR)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTWO_PS PRIMARY KEY (FNR)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTWO_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // HTHREE TO DEFAULT,VTWO,VTHREE,HTWO,HTHREE
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGHTHREE (\n"
                + "FNR        integer,\n"
                + "FLC        varchar(2),\n"
                + "FLNR       integer,\n"
                + "VON        varchar(3),\n"
                + "NACH       varchar(3),\n"
                + "AB             integer,\n"
                + "AN         integer,\n"
                + "constraint FLUGHTHREE_PS primary key (FNR),\n"
                + "constraint FLUGHTHREE_FS_VOND foreign key (VON) references FLUGHAFEND(FHC),\n"
                + "constraint FLUGHTHREE_FS_VONHTWO foreign key (VON) references FLUGHAFENHTWO(FHC),\n"
                + "constraint FLUGHTHREE_FS_VONHTHREE foreign key (VON) references FLUGHAFENHTHREE(FHC),\n"
                + "constraint FLUGHTHREE_FS_VONVTWO foreign key (VON) references FLUGHAFENVTWO(FHC),\n"
                + "constraint FLUGHTHREE_FS_VONVTHREE foreign key (VON) references FLUGHAFENVTHREE(FHC)\n"
                + ")\n"
                + "HORIZONTAL (FLC('KK','MM'))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTHREE_PS PRIMARY KEY (FNR)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTHREE_PS PRIMARY KEY (FNR)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGHTHREE_PS PRIMARY KEY (FNR)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGHTHREE_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // VTWO TO DEFAULT,VTWO,VTHREE,HTWO,HTHREE
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGVTWO (\n"
                + "FNR        integer,\n"
                + "FLC        varchar(2),\n"
                + "FLNR       integer,\n"
                + "VON        varchar(3),\n"
                + "NACH       varchar(3),\n"
                + "AB             integer,\n"
                + "AN         integer,\n"
                + "constraint FLUGVTWO_PS primary key (FNR),\n"
                + "constraint FLUGVTWO_FS_VOND foreign key (VON) references FLUGHAFEND(FHC),\n"
                + "constraint FLUGVTWO_FS_VONHTWO foreign key (VON) references FLUGHAFENHTWO(FHC),\n"
                + "constraint FLUGVTWO_FS_VONHTHREE foreign key (VON) references FLUGHAFENHTHREE(FHC),\n"
                + "constraint FLUGVTWO_FS_VONVTWO foreign key (VON) references FLUGHAFENVTWO(FHC),\n"
                + "constraint FLUGVTWO_FS_VONVTHREE foreign key (VON) references FLUGHAFENVTHREE(FHC),\n"
                + "constraint FLUGVTWO_FS_NACHD foreign key (NACH) references FLUGHAFEND(FHC),\n"
                + "constraint FLUGVTWO_FS_NACHHTWO foreign key (NACH) references FLUGHAFENHTWO(FHC),\n"
                + "constraint FLUGVTWO_FS_NACHHTHREE foreign key (NACH) references FLUGHAFENHTHREE(FHC),\n"
                + "constraint FLUGVTWO_FS_NACHVTWO foreign key (NACH) references FLUGHAFENVTWO(FHC),\n"
                + "constraint FLUGVTWO_FS_NACHVTHREE foreign key (NACH) references FLUGHAFENVTHREE(FHC)\n"
                + ")\n"
                + "VERTICAL ((FLC, VON, FNR),(AN, AB, NACH, FLNR))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_PS PRIMARY KEY (FNR)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_PS PRIMARY KEY (FNR)"));

        // FK ON PARTITION 1
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));
        // FK ON PARTITION 2
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHD FOREIGN KEY (NACH) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHD FOREIGN KEY (NACH) REFERENCES FLUGHAFEND(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHHTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHHTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTWO(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHHTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHHTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTHREE(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHVTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHVTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTWO(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHVTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTWO_FS_NACHVTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTHREE(FHC)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

        // VTWO TO DEFAULT,VTWO,VTHREE,HTWO,HTHREE
        Logger.logln("___________________________________________________________________________________");
        ast = parseStatement("create table FLUGVTHREE (\n"
                + "FNR        integer,\n"
                + "FLC        varchar(2),\n"
                + "FLNR       integer,\n"
                + "VON        varchar(3),\n"
                + "NACH       varchar(3),\n"
                + "BLA       varchar(3),\n"
                + "AB             integer,\n"
                + "AN         integer,\n"
                + "constraint FLUGVTHREE_PS primary key (FNR),\n"
                + "constraint FLUGVTHREE_FS_VOND foreign key (VON) references FLUGHAFEND(FHC),\n"
                + "constraint FLUGVTHREE_FS_VONHTWO foreign key (VON) references FLUGHAFENHTWO(FHC),\n"
                + "constraint FLUGVTHREE_FS_VONHTHREE foreign key (VON) references FLUGHAFENHTHREE(FHC),\n"
                + "constraint FLUGVTHREE_FS_VONVTWO foreign key (VON) references FLUGHAFENVTWO(FHC),\n"
                + "constraint FLUGVTHREE_FS_VONVTHREE foreign key (VON) references FLUGHAFENVTHREE(FHC),\n"
                + "constraint FLUGVTHREE_FS_NACHD foreign key (NACH) references FLUGHAFEND(FHC),\n"
                + "constraint FLUGVTHREE_FS_NACHHTWO foreign key (NACH) references FLUGHAFENHTWO(FHC),\n"
                + "constraint FLUGVTHREE_FS_NACHHTHREE foreign key (NACH) references FLUGHAFENHTHREE(FHC),\n"
                + "constraint FLUGVTHREE_FS_NACHVTWO foreign key (NACH) references FLUGHAFENVTWO(FHC),\n"
                + "constraint FLUGVTHREE_FS_NACHVTHREE foreign key (NACH) references FLUGHAFENVTHREE(FHC),\n"
                + "constraint FLUGVTHREE_FS_BLAD foreign key (BLA) references FLUGHAFEND(FHC),\n"
                + "constraint FLUGVTHREE_FS_BLAHTWO foreign key (BLA) references FLUGHAFENHTWO(FHC),\n"
                + "constraint FLUGVTHREE_FS_BLAHTHREE foreign key (BLA) references FLUGHAFENHTHREE(FHC),\n"
                + "constraint FLUGVTHREE_FS_BLAVTWO foreign key (BLA) references FLUGHAFENVTWO(FHC),\n"
                + "constraint FLUGVTHREE_FS_BLAVTHREE foreign key (BLA) references FLUGHAFENVTHREE(FHC)\n"
                + ")\n"
                + "VERTICAL ((AN, VON, FNR),(FLC, FLNR, BLA),(NACH, AB))");
        executerTask = resolveStatement(ast);

        Assert.assertTrue(!getDb1SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb2SubTasks(executerTask).isEmpty());
        Assert.assertTrue(!getDb3SubTasks(executerTask).isEmpty());

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_PS PRIMARY KEY (FNR)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_PS PRIMARY KEY (FNR)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_PS PRIMARY KEY (FNR)"));

        // FK ON PARTITION 1 (VON)
        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VOND FOREIGN KEY (VON) REFERENCES FLUGHAFEND(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONHTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENHTWO(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONHTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENHTHREE(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONVTWO FOREIGN KEY (VON) REFERENCES FLUGHAFENVTWO(FHC)"));

        Assert.assertTrue(getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_VONVTHREE FOREIGN KEY (VON) REFERENCES FLUGHAFENVTHREE(FHC)"));
        // FK ON PARTITION 2 (BLA))
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAD FOREIGN KEY (BLA) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAD FOREIGN KEY (BLA) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAD FOREIGN KEY (BLA) REFERENCES FLUGHAFEND(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAHTWO FOREIGN KEY (BLA) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAHTWO FOREIGN KEY (BLA) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAHTWO FOREIGN KEY (BLA) REFERENCES FLUGHAFENHTWO(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAHTHREE FOREIGN KEY (BLA) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAHTHREE FOREIGN KEY (BLA) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAHTHREE FOREIGN KEY (BLA) REFERENCES FLUGHAFENHTHREE(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAVTWO FOREIGN KEY (BLA) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAVTWO FOREIGN KEY (BLA) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAVTWO FOREIGN KEY (BLA) REFERENCES FLUGHAFENVTWO(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAVTHREE FOREIGN KEY (BLA) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAVTHREE FOREIGN KEY (BLA) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_BLAVTHREE FOREIGN KEY (BLA) REFERENCES FLUGHAFENVTHREE(FHC)"));
        // FK ON PARTITION 3 (NACH)
        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHD FOREIGN KEY (NACH) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHD FOREIGN KEY (NACH) REFERENCES FLUGHAFEND(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHD FOREIGN KEY (NACH) REFERENCES FLUGHAFEND(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHHTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHHTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTWO(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHHTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTWO(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHHTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHHTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTHREE(FHC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHHTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENHTHREE(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHVTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHVTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTWO(FHC)"));
        Assert.assertTrue(!getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHVTWO FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTWO(FHC)"));

        Assert.assertTrue(!getDb1Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHVTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(!getDb2Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHVTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTHREE(FHC)"));
        Assert.assertTrue(getDb3Query(executerTask).contains("CONSTRAINT FLUGVTHREE_FS_NACHVTHREE FOREIGN KEY (NACH) REFERENCES FLUGHAFENVTHREE(FHC)"));

        Assert.assertEquals(0, executeStatement(executerTask, ast));

    }

    protected AST parseStatement(String statement) throws FedException {
        AST ast = null;
        try {
            SqlParser sqlParser = new SqlParser(statement);
            ast = sqlParser.parseStatement();
            ast.getRoot().accept(new SemanticValidator(fedCon));
        } catch (ParseException ex) {
            java.util.logging.Logger.getLogger(DDLStatementResolverTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(DDLStatementResolverTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ast;
    }

    protected void executeStatement(String stmt, Integer db) throws FedException, SQLException {
        SQLExecuter sqlExecuter = new SQLExecuter(fedCon);
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

    protected SQLExecuterTask resolveStatement(AST ast) throws FedException {
        StatementResolverManager statementResolver = new StatementResolverManager(fedCon);
        return statementResolver.resolveStatement(ast);
    }

    protected int executeStatement(SQLExecuterTask executerTask, AST ast) throws FedException {
        int result = 0;
        try {
            SQLExecuter sqlExecuter = new SQLExecuter(fedCon);
            sqlExecuter.executeTask(executerTask);
            ExecuteUpdateUnifierManager statementUnifierManager = new ExecuteUpdateUnifierManager(fedCon);
            result = statementUnifierManager.unifyUpdateResult(ast, executerTask);
        } catch (SQLException ex) {
            java.util.logging.Logger.getLogger(DDLStatementResolverTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }

    protected List<SQLExecuterSubTask> getDb1SubTasks(SQLExecuterTask execTask) throws FedException {
        List<SQLExecuterSubTask> execSubTaskList = execTask.getDb1Statements();
        return execSubTaskList;
    }

    protected List<SQLExecuterSubTask> getDb2SubTasks(SQLExecuterTask execTask) throws FedException {
        List<SQLExecuterSubTask> execSubTaskList = execTask.getDb2Statements();
        return execSubTaskList;
    }

    protected List<SQLExecuterSubTask> getDb3SubTasks(SQLExecuterTask execTask) throws FedException {
        List<SQLExecuterSubTask> execSubTaskList = execTask.getDb3Statements();
        return execSubTaskList;
    }

    protected String getDb1Query(SQLExecuterTask execTask) throws FedException {
        String query = execTask.getDb1Statements().get(0).getQuery();
        return query;
    }

    protected String getDb2Query(SQLExecuterTask execTask) throws FedException {
        String query = execTask.getDb2Statements().get(0).getQuery();
        return query;
    }

    protected String getDb3Query(SQLExecuterTask execTask) throws FedException {
        String query = execTask.getDb3Statements().get(0).getQuery();
        return query;
    }
}
