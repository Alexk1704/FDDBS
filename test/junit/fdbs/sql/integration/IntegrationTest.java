package junit.fdbs.sql.integration;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedPseudoDriver;
import fdbs.sql.FedResultSet;
import fdbs.sql.FedStatement;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterSubTask;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.statement.DeleteStatement;
import fdbs.sql.parser.ast.statement.DropTableStatement;
import fdbs.sql.parser.ast.statement.InsertStatement;
import fdbs.sql.parser.ast.statement.UpdateStatement;
import fdbs.sql.parser.ast.statement.select.SelectCountAllTableStatement;
import fdbs.sql.parser.ast.statement.select.SelectNoGroupStatement;
import fdbs.sql.resolver.StatementResolverException;
import fdbs.sql.resolver.StatementResolverManager;
import fdbs.sql.semantic.SemanticValidationException;
import fdbs.sql.semantic.SemanticValidator;
import fdbs.sql.unifier.ExecuteUpdateUnifierManager;
import fdbs.util.logger.Logger;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import junit.fdbs.sql.resolver.DDLStatementResolverTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IntegrationTest {

    protected static FedConnection fedCon;
    protected static MetadataManager metadataManager;

    public IntegrationTest() {
    }

    @BeforeClass
    public static void setUpClass() throws FedException {
        fedCon = new FedPseudoDriver().getConnection("VDBSA03", "VDBSA03");
        metadataManager = MetadataManager.getInstance(fedCon);
    }

    @AfterClass
    public static void tearDownClass() throws FedException {
        fedCon.close();
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
    }

    @After
    public void tearDown() {
        Logger.infoln("FedTestEnvironment tests finished...");
    }

    @Test
    public void testSystem() {
        try {
            this.fedCon.setAutoCommit(false);
            this.executeStatements(fedCon, "Test/DRPTABS.SQL");
            this.executeStatements(fedCon, "Test/CREPARTABS.SQL");
            this.executeStatements(fedCon, "Test/INSERTAIRPORTS.SQL");
            this.executeStatements(fedCon, "Test/INSERTAIRLINES.SQL");
            this.executeStatements(fedCon, "Test/INSERTPASSENGERS.SQL");
            this.executeStatements(fedCon, "Test/INSERTFLIGHTS.SQL");
            this.executeStatements(fedCon, "Test/INSERTBOOKINGS.SQL");
            this.executeStatements(fedCon, "Test/PARSELCNTSTAR.SQL");
            this.executeStatements(fedCon, "Test/PARSELS1T.SQL");
            this.executeStatements(fedCon, "Test/PARSELS1OR.SQL");
            this.executeStatements(fedCon, "Test/PARSELSJOIN1.SQL"); // TODO: Tested by pat
//            this.executeStatements(fedCon, "Test/PARSELS1TGP.SQL");
//            this.executeStatements(fedCon, "Test/PARSELS1TWGP.SQL");
//            this.executeStatements(fedCon, "Test/PARSELS1TGHAV.SQL");
            //this.executeStatements(fedCon, "Test/PARUPDS.SQL"); //tested
            //this.executeStatements(fedCon, "Test/PARDELS.SQL"); //tested
            //this.executeStatements(fedCon, "Test/PARINSERTS.SQL"); //tested
            this.executeStatements(fedCon, "Test/PARSELCNTSTAR.SQL");
        } catch (FedException e) {
            System.out.println(e.getMessage());
        } catch (FileNotFoundException ex) {
            java.util.logging.Logger.getLogger(IntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void executeStatements(FedConnection fedConnection, String filename) throws FileNotFoundException {
        System.out.println("**************************************************************************");
        System.out.println("Executing script file '" + filename + "'...");

        long op = 0;
        long start = System.currentTimeMillis();

        //final Scanner scanner = new Scanner(getClass().getClassLoader().getResourceAsStream(filename)).useDelimiter(";");
        final Scanner scanner = new Scanner(new File("src/fdbs/app/federated/" + filename)).useDelimiter(";");
        while (scanner.hasNext()) {

            String statement = scanner.next().trim();
            while (statement.startsWith("/*") || statement.startsWith("--")) {
                if (statement.startsWith("/*")) {
                    final String comment = statement.substring(0, statement.indexOf("*/") + 2);
                    if (true) {
                        System.out.println("--> " + comment + " <--");
                    }
                    statement = statement.substring(statement.indexOf("*/") + 2).trim();
                } else {
                    final String comment = statement.substring(0, statement.indexOf("\n") - 1);
                    if (true) {
                        System.out.println("--> " + comment + " <--");
                    }
                    statement = statement.substring(statement.indexOf("\n") + 1).trim();
                }
            }

            if (!"".equals(statement)) {
                if (!"SET ECHO ON".equals(statement.toUpperCase()) && !statement.toUpperCase().startsWith("ALTER SESSION")) {
                    if (true) {
                        System.out.println("Executing \"" + statement + "\"...\n");
                        System.out.flush();
                    }
                    if (statement.toUpperCase().equals("COMMIT")) {
                        try {
                            fedConnection.commit();
                            if (true) {
                                System.out.println("Transaction commit");
                            }
                        } catch (final FedException fedException) {
                            System.out.println(fedException.getMessage());
                            System.out.flush();
                        }
                    } else {
                        if (statement.toUpperCase().equals("ROLLBACK")) {
                            try {
                                fedConnection.rollback();
                                if (true) {
                                    System.out.println("Transaction rollback");
                                }
                            } catch (final FedException fedException) {
                                System.out.println(fedException.getMessage());
                                System.out.flush();
                            }
                        } else {
                            if (statement.toUpperCase().startsWith("SELECT")) {

                                // SELECT
                                try {
                                    AST ast = parseStatement(statement);
                                    final FedStatement fedStatement = fedConnection.getStatement();
                                    final FedResultSet fedResultSet = fedStatement.executeQuery(statement);

                                    op++;

                                    if (statement.startsWith("SELECT COUNT(*)")) {
                                        this.checkSelectCountAll((SelectCountAllTableStatement) ast.getRoot(), fedResultSet);
                                    }

                                    if (filename.equals("Test/PARSELS1T.SQL")) {
                                        this.checkSelectS1T((SelectNoGroupStatement) ast.getRoot(), fedResultSet);
                                    }
                                    
                                    if(filename.equals("Test/PARSELS1OR.SQL"))
                                    {
                                        this.checkSelectOr((SelectNoGroupStatement) ast.getRoot(), fedResultSet);
                                    }
                                    
                                    fedStatement.close();
                                } catch (final FedException fedException) {
                                    System.out.println(fedException.getMessage());
                                    System.out.flush();
                                }
                            } else {
                                // UPDATE, INSERT, DELETE, CREATE, DROP
                                try {
                                    AST ast = parseStatement(statement);
                                    ast.getRoot().accept(new SemanticValidator(fedConnection));
                                    SQLExecuterTask executerTask = resolveStatement(ast);
                                    int result = executeStatement(executerTask, ast);
                                    if (statement.startsWith("INSERT")) {
                                        if (statement.startsWith("INSERT INTO FLUGLINIE")) {
                                            this.checkInsertedAirlines((InsertStatement) ast.getRoot(), executerTask, result);
                                        } else {
                                            if (statement.startsWith("INSERT INTO FLUGHAFEN")) {
                                                this.checkInsertedAirports((InsertStatement) ast.getRoot(), executerTask, result);
                                            } else {
                                                if (statement.startsWith("INSERT INTO BUCHUNG")) {
                                                    this.checkInsertedBookings((InsertStatement) ast.getRoot(), executerTask, result);
                                                } else {
                                                    if (statement.startsWith("INSERT INTO FLUG")) {
                                                        this.checkInsertedFlights((InsertStatement) ast.getRoot(), executerTask, result);
                                                    } else {
                                                        if (statement.startsWith("INSERT INTO PASSAGIER")) {
                                                            this.checkInsertedPassengers((InsertStatement) ast.getRoot(), executerTask, result);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else if (statement.startsWith("update") || statement.startsWith("UPDATE")) {
                                        if (statement.startsWith("update FLUGLINIE")) {
                                            this.checkUpdateAirlines((UpdateStatement) ast.getRoot(), executerTask, result);
                                        } else {
                                            if (statement.startsWith("UPDATE FLUG")) {
                                                this.checkUpdateFlug((UpdateStatement) ast.getRoot(), executerTask, result);
                                            } else if (statement.startsWith("UPDATE BUCHUNG")) {
                                                this.checkUpdateBuchung((UpdateStatement) ast.getRoot(), executerTask, result);
                                            }
                                        }
                                    } else if (statement.startsWith("CREATE")) {
                                        Assert.assertTrue("CREATE worked.", result == 0);
                                    } else if (statement.startsWith("drop")) {
                                        this.checkDropTables((DropTableStatement) ast.getRoot(), executerTask, result);
                                    } else if (statement.startsWith("DELETE")) {
                                        this.checkDeleteOnBuchung((DeleteStatement) ast.getRoot(), executerTask, result);
                                    }
                                } catch (StatementResolverException | SemanticValidationException | SQLException | FedException fedException) {
                                    System.out.println(fedException.getMessage());
                                    // TODO: ggf. manuelle Prüfung der fehlerhaften Inserts
                                    System.out.flush();
                                } catch (Exception ex) {
                                    System.out.println(ex.getMessage());
                                    System.out.flush();
                                }
                            }
                        }
                    }
                }
            }
        }

        final long end = System.currentTimeMillis();
        final long delta = end - start;

        System.out.println("File '" + filename + "', " + op + " operations, " + delta + " milliseconds");
        System.out.println("**************************************************************************\n");
    }

    protected SQLExecuterTask resolveStatement(AST ast) throws FedException {
        StatementResolverManager statementResolver = new StatementResolverManager(fedCon);
        return statementResolver.resolveStatement(ast);
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

    private void checkInsertedAirlines(InsertStatement insert, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("Inserted Airline");
        Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
        switch (insert.getValues().get(0).getIdentifier()) {
            case "AB":
                Assert.assertTrue("INSERT AIRLINE should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUGLINIE VALUES ('AB','D  ',null,'Air Berlin',null)"));
                Assert.assertTrue("INSERT AIRLINE should have no Query on db2", execTask.getDb2Statements().isEmpty());
                Assert.assertTrue("INSERT AIRLINE should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            case "LH":
                Assert.assertTrue("INSERT AIRLINE should have no Query on db1", execTask.getDb1Statements().isEmpty());
                Assert.assertTrue("INSERT AIRLINE should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUGLINIE VALUES ('LH','D  ',null,'Lufthansa','Star')"));
                Assert.assertTrue("INSERT AIRLINE should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            case "UA":
                Assert.assertTrue("INSERT AIRLINE should have no Query on db1", execTask.getDb1Statements().isEmpty());
                Assert.assertTrue("INSERT AIRLINE should have no Query on db2", execTask.getDb2Statements().isEmpty());
                Assert.assertTrue("INSERT AIRLINE should have Query on db3", getDb3Query(execTask).contains("INSERT INTO FLUGLINIE VALUES ('UA','USA',null,'United Airlines','Star')"));
                break;
        }
    }

    private void checkUpdateAirlines(UpdateStatement update, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("Updated Airline");
        System.out.println("Count: " + count);
        if (update.getAttributeIdentifier().getIdentifier().equals("ALLIANZ")) {
            if (update.getWhereClause().getBinaryExpression().getRightOperand().getIdentifier().equals("DL")) {
                Assert.assertTrue("UPDATE AIRLINE should have Query on db1", getDb1Query(execTask).contains("UPDATE FLUGLINIE SET ALLIANZ = 'SkyTeam' WHERE FLC = 'DL'"));
                Assert.assertTrue("UPDATE AIRLINE should have Query on db2", getDb2Query(execTask).contains("UPDATE FLUGLINIE SET ALLIANZ = 'SkyTeam' WHERE FLC = 'DL'"));
                Assert.assertTrue("UPDATE AIRLINE should have Query on db3", getDb3Query(execTask).contains("UPDATE FLUGLINIE SET ALLIANZ = 'SkyTeam' WHERE FLC = 'DL'"));
                Assert.assertTrue("Update should have 1 row affected.", count == 1);
            }
            if (update.getWhereClause().getBinaryExpression().getRightOperand().getIdentifier().equals("JL")) {
                Assert.assertTrue("UPDATE AIRLINE should have Query on db1", getDb1Query(execTask).contains("UPDATE FLUGLINIE SET ALLIANZ = 'OneWorld' WHERE FLC = 'JL'"));
                Assert.assertTrue("UPDATE AIRLINE should have Query on db2", getDb2Query(execTask).contains("UPDATE FLUGLINIE SET ALLIANZ = 'OneWorld' WHERE FLC = 'JL'"));
                Assert.assertTrue("UPDATE AIRLINE should have Query on db3", getDb3Query(execTask).contains("UPDATE FLUGLINIE SET ALLIANZ = 'OneWorld' WHERE FLC = 'JL'"));
                Assert.assertTrue("Update should have 1 row affected.", count == 1);
            }
        } else {
            switch (update.getConstant().getIdentifier()) {
                case "FRA":
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db1", getDb1Query(execTask).contains("UPDATE FLUGLINIE SET HUB = 'FRA' WHERE FLC = 'LH'"));
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db2", getDb2Query(execTask).contains("UPDATE FLUGLINIE SET HUB = 'FRA' WHERE FLC = 'LH'"));
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db3", getDb3Query(execTask).contains("UPDATE FLUGLINIE SET HUB = 'FRA' WHERE FLC = 'LH'"));
                    Assert.assertTrue("Update should have 1 row affected.", count == 1);
                    break;
                case "NRT":
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db1", getDb1Query(execTask).contains("UPDATE FLUGLINIE SET HUB = 'NRT' WHERE FLC = 'JL'"));
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db2", getDb2Query(execTask).contains("UPDATE FLUGLINIE SET HUB = 'NRT' WHERE FLC = 'JL'"));
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db3", getDb3Query(execTask).contains("UPDATE FLUGLINIE SET HUB = 'NRT' WHERE FLC = 'JL'"));
                    break;
                case "NULL":
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db1", getDb1Query(execTask).contains("UPDATE FLUGLINIE SET HUB = NULL"));
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db2", getDb2Query(execTask).contains("UPDATE FLUGLINIE SET HUB = NULL"));
                    Assert.assertTrue("UPDATE AIRLINE should have Query on db3", getDb3Query(execTask).contains("UPDATE FLUGLINIE SET HUB = NULL"));
                    Assert.assertTrue("Update should have 11 rows affected.", count == 11);
                    break;
                default:
                    break;
            }
        }
    }

    private void checkUpdateBuchung(UpdateStatement update, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("Updated Buchung");
        switch (update.getConstant().getIdentifier()) {
            case "01-JAN-2014":
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db1", getDb1Query(execTask).contains("UPDATE BUCHUNG SET TAG = '01-JAN-2014' WHERE BNR = 80"));
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db2", getDb2Query(execTask).contains("UPDATE BUCHUNG SET TAG = '01-JAN-2014' WHERE BNR = 80"));
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db3", getDb3Query(execTask).contains("UPDATE BUCHUNG SET TAG = '01-JAN-2014' WHERE BNR = 80"));
                Assert.assertTrue("UPDATE should have updated 1 row.", count == 1);
                break;
            case "01-FEB-2015":
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db1", getDb1Query(execTask).contains("UPDATE BUCHUNG SET TAG = '01-FEB-2015' WHERE VON = 'FRA'"));
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db2", getDb2Query(execTask).contains("UPDATE BUCHUNG SET TAG = '01-FEB-2015' WHERE VON = 'FRA'"));
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db3", getDb3Query(execTask).contains("UPDATE BUCHUNG SET TAG = '01-FEB-2015' WHERE VON = 'FRA'"));
                Assert.assertTrue("UPDATE should have updated 95 row.", count == 95);
                break;
            case "1000":
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db1", getDb1Query(execTask).contains("UPDATE BUCHUNG SET MEILEN = 1000"));
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db2", getDb2Query(execTask).contains("UPDATE BUCHUNG SET MEILEN = 1000"));
                Assert.assertTrue("UPDATE BUCHUNG should have Query on db3", getDb3Query(execTask).contains("UPDATE BUCHUNG SET MEILEN = 1000"));
                Assert.assertTrue("UPDATE should have updated 188 row.", count == 188);
                break;
            default:
                break;

        }
    }

    private void checkUpdateFlug(UpdateStatement update, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("Updated Flug");
        switch (update.getConstant().getIdentifier()) {
            case "0700":
                Assert.assertTrue("UPDATE FLUG should have Query on db1", getDb1Query(execTask).contains("UPDATE FLUG SET AB = 700 WHERE FNR = 52"));
                Assert.assertTrue("UPDATE FLUG should have Query on db2", getDb2Query(execTask).contains("UPDATE FLUG SET AB = 700 WHERE FNR = 52"));
                Assert.assertTrue("UPDATE FLUG should have Query on db3", getDb3Query(execTask).contains("UPDATE FLUG SET AB = 700 WHERE FNR = 52"));
                Assert.assertTrue("UPDATE should have updated 1 row.", count == 1);
                break;
            case "1120":
                Assert.assertTrue("UPDATE FLUG should have Query on db1", getDb1Query(execTask).contains("UPDATE FLUG SET AB = 1120 WHERE FNR = 3"));
                Assert.assertTrue("UPDATE FLUG should have Query on db2", getDb2Query(execTask).contains("UPDATE FLUG SET AB = 1120 WHERE FNR = 3"));
                Assert.assertTrue("UPDATE FLUG should have Query on db3", getDb3Query(execTask).contains("UPDATE FLUG SET AB = 1120 WHERE FNR = 3"));
                Assert.assertTrue("UPDATE should have updated 1 row.", count == 1);
                break;
            default:
                break;
        }
    }

    private void checkInsertedFlights(InsertStatement insert, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("Inserted Flight");
        switch (insert.getValues().get(0).getIdentifier()) {
            case "91":
                Assert.assertTrue("INSERT FLIGHT should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUG VALUES (91,'AC',10,'YYZ','FRA',1815,740)"));
                Assert.assertTrue("INSERT FLIGHT should have no Query on db2", execTask.getDb2Statements().isEmpty());
                Assert.assertTrue("INSERT FLIGHT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            case "47":
                Assert.assertTrue("INSERT FLIGHT should have no Query on db1", execTask.getDb1Statements().isEmpty());
                Assert.assertTrue("INSERT FLIGHT should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUG VALUES (47,'LH',9,'FRA','TXL',1430,1530)"));
                Assert.assertTrue("INSERT FLIGHT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            case "300":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "301":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "302":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "303":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "304":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "305":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "306":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "307":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "308":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "309":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "310":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "311":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "312":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "313":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "314":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "315":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "088":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            default:
                break;
        }
    }

    private void checkInsertedPassengers(InsertStatement insert, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("Inserted Passenger");
        switch (insert.getValues().get(0).getIdentifier()) {
            case "1":
                Assert.assertTrue("INSERT PASSENGER should have Query on db1", getDb1Query(execTask).contains("INSERT INTO PASSAGIER VALUES (1,'Collins')"));
                Assert.assertTrue("INSERT PASSENGER should have Query on db2", getDb2Query(execTask).contains("INSERT INTO PASSAGIER VALUES (1,'Phil','GB ')"));
                Assert.assertTrue("INSERT PASSENGER should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            case "14":
                Assert.assertTrue("INSERT PASSENGER should have Query on db1", getDb1Query(execTask).contains("INSERT INTO PASSAGIER VALUES (14,'John')"));
                Assert.assertTrue("INSERT PASSENGER should have Query on db2", getDb2Query(execTask).contains("INSERT INTO PASSAGIER VALUES (14,'Elton',null)"));
                Assert.assertTrue("INSERT PASSENGER should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            case "27":
                Assert.assertTrue("INSERT PASSENGER should have Query on db1", getDb1Query(execTask).contains("INSERT INTO PASSAGIER VALUES (27,'Hennecke')"));
                Assert.assertTrue("INSERT PASSENGER should have Query on db2", getDb2Query(execTask).contains("INSERT INTO PASSAGIER VALUES (27,'Christian',null)"));
                Assert.assertTrue("INSERT PASSENGER should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            case "42":
                Assert.assertTrue("INSERT PASSENGER should have Query on db1", getDb1Query(execTask).contains("INSERT INTO PASSAGIER VALUES (42,'Bostanci')"));
                Assert.assertTrue("INSERT PASSENGER should have Query on db2", getDb2Query(execTask).contains("INSERT INTO PASSAGIER VALUES (42,'Sinan','D  ')"));
                Assert.assertTrue("INSERT PASSENGER should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            case "57":
                Assert.assertTrue("INSERT PASSENGER should have Query on db1", getDb1Query(execTask).contains("INSERT INTO PASSAGIER VALUES (57,'Paulheim')"));
                Assert.assertTrue("INSERT PASSENGER should have Query on db2", getDb2Query(execTask).contains("INSERT INTO PASSAGIER VALUES (57,'Annika','D  ')"));
                Assert.assertTrue("INSERT PASSENGER should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            default:
                break;
        }
    }

    private void checkInsertedBookings(InsertStatement insert, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("Inserted Booking");
        switch (insert.getValues().get(0).getIdentifier()) {
            case "71":
                Assert.assertTrue("INSERT BUCHUNG should have Query on db1", getDb1Query(execTask).contains("INSERT INTO BUCHUNG VALUES (71,1,'LH',32,'FRA','LAX','03-SEP-1997',5800,1200)"));
                Assert.assertTrue("INSERT BUCHUNG should have no Query on db2", execTask.getDb2Statements().isEmpty());
                Assert.assertTrue("INSERT BUCHUNG should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            case "61":
                Assert.assertTrue("INSERT BUCHUNG should have no Query on db1", execTask.getDb1Statements().isEmpty());
                Assert.assertTrue("INSERT BUCHUNG should have Query on db2", getDb2Query(execTask).contains("INSERT INTO BUCHUNG VALUES (61,56,'DB',36,'HAJ','FRA','04-FEB-2011',400,500)"));
                Assert.assertTrue("INSERT BUCHUNG should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
            case "153":
                Assert.assertTrue("INSERT BUCHUNG should have no Query on db1", execTask.getDb1Statements().isEmpty());
                Assert.assertTrue("INSERT BUCHUNG should have no Query on db2", execTask.getDb2Statements().isEmpty());
                Assert.assertTrue("INSERT BUCHUNG should have Query on db3", getDb3Query(execTask).contains("INSERT INTO BUCHUNG VALUES (153,71,'DB',10,'DJE','FRA','06-JAN-2010',2800,200)"));
                Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
                break;
//            case "184":
//                Assert.assertTrue("Insert should not have worked", count == 0);
//                break;
            case "200":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "201":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "202":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "203":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "204":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            case "205":
                Assert.assertTrue("Insert should not have worked", count == 0);
                break;
            default:
                break;
        }
    }

    private void checkInsertedAirports(InsertStatement insert, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("Inserted Airport");
        Assert.assertTrue("Insert should have inserted 1 Line", count == 1);
        switch (insert.getValues().get(0).getIdentifier()) {
            case "AKL":
                Assert.assertTrue("INSERT AIRPORT should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('AKL','NZ ')"));
                Assert.assertTrue("INSERT AIRPORT should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('AKL','Auckland','Auckland International')"));
                Assert.assertTrue("INSERT AIRPORT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            case "CPH":
                Assert.assertTrue("INSERT AIRPORT should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('CPH','DK ')"));
                Assert.assertTrue("INSERT AIRPORT should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('CPH','Kopenhagen','Kastrup')"));
                Assert.assertTrue("INSERT AIRPORT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            case "GRZ":
                Assert.assertTrue("INSERT AIRPORT should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('GRZ','A  ')"));
                Assert.assertTrue("INSERT AIRPORT should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('GRZ','Graz','Flughafen Graz')"));
                Assert.assertTrue("INSERT AIRPORT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            case "LAX":
                Assert.assertTrue("INSERT AIRPORT should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('LAX','USA')"));
                Assert.assertTrue("INSERT AIRPORT should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('LAX','Los Angeles','')"));
                Assert.assertTrue("INSERT AIRPORT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            case "NAP":
                Assert.assertTrue("INSERT AIRPORT should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('NAP','I  ')"));
                Assert.assertTrue("INSERT AIRPORT should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('NAP','Neapel','Capodichino')"));
                Assert.assertTrue("INSERT AIRPORT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            case "STN":
                Assert.assertTrue("INSERT AIRPORT should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('STN','GB ')"));
                Assert.assertTrue("INSERT AIRPORT should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('STN','London','Stanstead')"));
                Assert.assertTrue("INSERT AIRPORT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            case "YUL":
                Assert.assertTrue("INSERT AIRPORT should have Query on db1", getDb1Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('YUL','CDN')"));
                Assert.assertTrue("INSERT AIRPORT should have Query on db2", getDb2Query(execTask).contains("INSERT INTO FLUGHAFEN VALUES ('YUL','Montreal','Pierre Elliot Trudeau')"));
                Assert.assertTrue("INSERT AIRPORT should have no Query on db3", execTask.getDb3Statements().isEmpty());
                break;
            default:
                break;
        }
    }

    private void checkDropTables(DropTableStatement drop, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("DROP Table detected");
        Assert.assertTrue("DROP worked.", count == 0);
        switch (drop.getTableIdentifier().getIdentifier()) {
            case "PASSAGIER":
                Assert.assertTrue("DROP PASSAGIER should have Query on db1", getDb1Query(execTask).contains("DROP TABLE PASSAGIER CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP PASSAGIER should have Query on db2", getDb2Query(execTask).contains("DROP TABLE PASSAGIER CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP PASSAGIER should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("There should be Tables left", metadataManager.getAllTables().size() == 3);
                break;
            case "BUCHUNG":
                Assert.assertTrue("DROP BUCHUNG should have Query on db1", getDb1Query(execTask).contains("DROP TABLE BUCHUNG CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP BUCHUNG should have Query on db2", getDb2Query(execTask).contains("DROP TABLE BUCHUNG CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP BUCHUNG should have Query on db3", getDb3Query(execTask).contains("DROP TABLE BUCHUNG CASCADE CONSTRAINTS"));
                Assert.assertTrue("There should be Tables left", metadataManager.getAllTables().size() == 4);
                break;
            case "FLUG":
                Assert.assertTrue("DROP FLUG should have Query on db1", getDb1Query(execTask).contains("DROP TABLE FLUG CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP FLUG should have Query on db2", getDb2Query(execTask).contains("DROP TABLE FLUG CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP FLUG should have Query on db3", getDb3Query(execTask).contains("DROP TABLE FLUG CASCADE CONSTRAINTS"));
                Assert.assertTrue("There should be Tables left", metadataManager.getAllTables().size() == 2);
                break;
            case "FLUGLINIE":
                Assert.assertTrue("DROP FLUGLINIE should have Query on db1", getDb1Query(execTask).contains("DROP TABLE FLUGLINIE CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP FLUGLINIE should have Query on db2", getDb2Query(execTask).contains("DROP TABLE FLUGLINIE CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP FLUGLINIE should have Query on db3", getDb3Query(execTask).contains("DROP TABLE FLUGLINIE CASCADE CONSTRAINTS"));
                Assert.assertTrue("There should be Tables left", metadataManager.getAllTables().size() == 1);
                break;
            case "FLUGHAFEN":
                Assert.assertTrue("DROP FLUGHAFEN should have Query on db1", getDb1Query(execTask).contains("DROP TABLE FLUGHAFEN CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP FLUGHAFEN should have Query on db2", getDb2Query(execTask).contains("DROP TABLE FLUGHAFEN CASCADE CONSTRAINTS"));
                Assert.assertTrue("DROP FLUGHAFEN should have no Query on db3", execTask.getDb3Statements().isEmpty());
                Assert.assertTrue("There should be Tables left", metadataManager.getAllTables().isEmpty());
                break;
            default:
                break;
        }
    }

    private void checkDeleteOnBuchung(DeleteStatement delete, SQLExecuterTask execTask, int count) throws FedException {
        System.out.println("DELETE on Buchung detected");
        if (delete.getWhereClause() != null) {
            switch (delete.getWhereClause().getBinaryExpression().getRightOperand().getIdentifier()) {
                case "80":
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have Query on db1", getDb1Query(execTask).contains("DELETE FROM BUCHUNG WHERE BNR = 80"));
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have no Query on db2", getDb2Query(execTask).contains("DELETE FROM BUCHUNG WHERE BNR = 80"));
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have no Query on db3", getDb3Query(execTask).contains("DELETE FROM BUCHUNG WHERE BNR = 80"));
                    Assert.assertTrue("There should be 1 row deleted.", count == 1);
                    break;
                case "CDG":
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have Query on db1", getDb1Query(execTask).contains("DELETE FROM BUCHUNG WHERE VON = 'CDG'"));
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have no Query on db2", getDb2Query(execTask).contains("DELETE FROM BUCHUNG WHERE VON = 'CDG'"));
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have no Query on db3", getDb3Query(execTask).contains("DELETE FROM BUCHUNG WHERE VON = 'CDG'"));
                    Assert.assertTrue("There should be 15 row deleted.", count == 15);
                    break;
                case "DL":
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have Query on db1", getDb1Query(execTask).contains("DELETE FROM BUCHUNG WHERE FLC = 'DL'"));
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have no Query on db2", getDb2Query(execTask).contains("DELETE FROM BUCHUNG WHERE FLC = 'DL'"));
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have no Query on db3", getDb3Query(execTask).contains("DELETE FROM BUCHUNG WHERE FLC = 'DL'"));
                    Assert.assertTrue("There should be 6 rows deleted.", count == 6);
                    break;
                case "DB":
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have Query on db1", getDb1Query(execTask).contains("DELETE FROM BUCHUNG WHERE FLC = 'DB'"));
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have no Query on db2", getDb2Query(execTask).contains("DELETE FROM BUCHUNG WHERE FLC = 'DB'"));
                    Assert.assertTrue("DELETE FROM BUCHUNGEN should have no Query on db3", getDb3Query(execTask).contains("DELETE FROM BUCHUNG WHERE FLC = 'DB'"));
                    Assert.assertTrue("There should 111 rows deleted.", count == 111);
                    break;
                default:
                    break;
            }
        } else {
            Assert.assertTrue("DELETE FROM BUCHUNGEN should have Query on db1", getDb1Query(execTask).contains("DELETE FROM BUCHUNG"));
            Assert.assertTrue("DELETE FROM BUCHUNGEN should have Query on db2", getDb2Query(execTask).contains("DELETE FROM BUCHUNG"));
            Assert.assertTrue("DELETE FROM BUCHUNGEN should have Query on db3", getDb3Query(execTask).contains("DELETE FROM BUCHUNG"));
            Assert.assertTrue("There should be rows deleted.", count == 55);
        }
    }

    private void checkSelectCountAll(SelectCountAllTableStatement select, FedResultSet resultSet) throws FedException {
        switch (select.getTableIdentifier().getIdentifier()) {
            case "FLUGLINIE":
                while (resultSet.next()) {
                    Assert.assertTrue("Count all on FLUGLINIE should have count 11", resultSet.getString(1).equals("11"));
                }
                break;
            case "FLUGHAFEN":
                while (resultSet.next()) {
                    Assert.assertTrue("Count all on FLUGHAFEN should have count 84", resultSet.getString(1).equals("84"));
                }
                break;
            case "FLUG":
                while (resultSet.next()) {
                    Assert.assertTrue("Count all on FLUG should have count 100", resultSet.getString(1).equals("100"));
                }
                break;
            case "PASSAGIER":
                while (resultSet.next()) {
                    Assert.assertTrue("Count all on PASSAGIER should have count 78", resultSet.getString(1).equals("78"));
                }
                break;
            case "BUCHUNG":
                while (resultSet.next()) {
                    Assert.assertTrue("Count all on BUCHUNG should have count 188", resultSet.getString(1).equals("188"));
                }
                break;
            default:
                break;
        }
    }
    
    private void checkSelectS1T(SelectNoGroupStatement select, FedResultSet resultSet) throws FedException {
        int i = 0;
        switch(select.getTableIdentifiers().get(0).getIdentifier())
        {
            case "PASSAGIER":
                switch(select.getWhereClause().getBinaryExpression().getOperator())
                {
                    case EQ:
                        Assert.assertTrue("SELECT should have 2 columns.", resultSet.getColumnCount() == 2);
                        while (resultSet.next())
                        {
                            Assert.assertTrue("First Column should have value 16", resultSet.getString(1).equals("16"));
                            Assert.assertTrue("Second Column should have value Horn", resultSet.getString(2).equals("Horn"));
                        }
                        break;
                    case OR:
                        Assert.assertTrue("SELECT should have 2 columns.", resultSet.getColumnCount() == 2);
                        while (resultSet.next())
                        {
                            if(i == 0)
                            {
                                Assert.assertTrue("First Column should have value 16", resultSet.getString(1).equals("16"));
                                Assert.assertTrue("Second Column should have value Horn", resultSet.getString(2).equals("Horn"));
                            }
                            else{
                                Assert.assertTrue("First Column should have value 38", resultSet.getString(1).equals("38"));
                                Assert.assertTrue("Second Column should have value Reinel", resultSet.getString(2).equals("Reinel"));
                            }
                            
                            i++;
                        }
                        break;
                    default:
                        break;
                }
                break;
            case "FLUG":
                Assert.assertTrue("SELECT should have 3 columns.", resultSet.getColumnCount() == 3);
                while (resultSet.next())
                {
                    if(i == 0)
                    {
                        Assert.assertTrue("First Column should have value DL", resultSet.getString(1).equals("DL"));
                        Assert.assertTrue("Second Column should have value 13", resultSet.getString(2).equals("13"));
                        Assert.assertTrue("Third Column should have value 1140", resultSet.getString(3).equals("1140"));
                    }
                    else if(i == 1){
                        Assert.assertTrue("First Column should have value DL", resultSet.getString(1).equals("DL"));
                        Assert.assertTrue("Second Column should have value 14", resultSet.getString(2).equals("14"));
                        Assert.assertTrue("Third Column should have value 2220", resultSet.getString(3).equals("2220"));
                    }
                    else{
                        Assert.assertTrue("First Column should have value DL", resultSet.getString(1).equals("DL"));
                        Assert.assertTrue("Second Column should have value 97", resultSet.getString(2).equals("97"));
                        Assert.assertTrue("Third Column should have value 900", resultSet.getString(3).equals("900"));
                    }

                    i++;
                }
                break;
            case "FLUGLINIE":
                if(select.getWhereClause() != null)
                {
                    switch(select.getWhereClause().getBinaryExpression().getOperator())
                    {
                        case AND:
                            Assert.assertTrue("SELECT should have 5 columns.", resultSet.getColumnCount() == 5);
                            while(resultSet.next())
                            {
                                Assert.assertTrue("First Column should have value LH", resultSet.getString(1).equals("LH"));
                                Assert.assertTrue("Second Column should have value D", resultSet.getString(2).startsWith("D"));
                                Assert.assertTrue("Third Column should have value FRA", resultSet.getString(3).startsWith("FRA"));
                                Assert.assertTrue("Forth Column should have value Lufthansa", resultSet.getString(4).equals("Lufthansa"));
                                Assert.assertTrue("Fifth Column should have value Star", resultSet.getString(5).equals("Star"));
                            }
                            
                            break;
                        case OR:
                            Assert.assertTrue("SELECT should have 5 columns.", resultSet.getColumnCount() == 5);
                            Assert.assertTrue("ResultSet should be empty", !resultSet.next());
                            break;
                        case EQ:
                            Assert.assertTrue("SELECT should have 5 columns.", resultSet.getColumnCount() == 5);
                            Assert.assertTrue("ResultSet should be empty", !resultSet.next());
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    while (resultSet.next())
                    {
                        Assert.assertTrue("SELECT should have 3 columns.", resultSet.getColumnCount() == 3);
                        switch(i){
                            case 0:
                                Assert.assertTrue("First Column should have value AB", resultSet.getString(1).equals("AB"));
                                Assert.assertTrue("Second Column should have value Air Berlin", resultSet.getString(2).equals("Air Berlin"));
                                Assert.assertTrue("Third Column should have value D", resultSet.getString(3).startsWith("D"));
                                break;
                            case 1:
                                Assert.assertTrue("First Column should have value AC", resultSet.getString(1).equals("AC"));
                                Assert.assertTrue("Second Column should have value Air Canada", resultSet.getString(2).equals("Air Canada"));
                                Assert.assertTrue("Third Column should have value CDN", resultSet.getString(3).startsWith("CDN"));
                                break;
                            case 2:
                                Assert.assertTrue("First Column should have value AF", resultSet.getString(1).equals("AF"));
                                Assert.assertTrue("Second Column should have value Air France", resultSet.getString(2).equals("Air France"));
                                Assert.assertTrue("Third Column should have value F", resultSet.getString(3).startsWith("F"));
                                break;
                            case 3:
                                Assert.assertTrue("First Column should have value BA", resultSet.getString(1).equals("BA"));
                                Assert.assertTrue("Second Column should have value Air France", resultSet.getString(2).equals("British Airways"));
                                Assert.assertTrue("Third Column should have value GB", resultSet.getString(3).startsWith("GB"));
                                break;
                            case 4:
                                Assert.assertTrue("First Column should have value DB", resultSet.getString(1).equals("DB"));
                                Assert.assertTrue("Second Column should have value Database Airline", resultSet.getString(2).equals("Database Airlines"));
                                Assert.assertTrue("Third Column should have value D", resultSet.getString(3).startsWith("D"));
                                break;
                            case 5:
                                Assert.assertTrue("First Column should have value DB", resultSet.getString(1).equals("DI"));
                                Assert.assertTrue("Second Column should have value Deutsche BA", resultSet.getString(2).equals("Deutsche BA"));
                                Assert.assertTrue("Third Column should have value D", resultSet.getString(3).startsWith("D"));
                                break;
                            case 6:
                                Assert.assertTrue("First Column should have value DL", resultSet.getString(1).equals("DL"));
                                Assert.assertTrue("Second Column should have value Delta Airlines", resultSet.getString(2).equals("Delta Airlines"));
                                Assert.assertTrue("Third Column should have value USA", resultSet.getString(3).startsWith("USA"));
                                break;
                            case 7:
                                Assert.assertTrue("First Column should have value JL", resultSet.getString(1).equals("JL"));
                                Assert.assertTrue("Second Column should have value Japan Airlines", resultSet.getString(2).equals("Japan Airlines"));
                                Assert.assertTrue("Third Column should have value D", resultSet.getString(3).startsWith("J"));
                                break;
                            case 8:
                                Assert.assertTrue("First Column should have value LH", resultSet.getString(1).equals("LH"));
                                Assert.assertTrue("Second Column should have value Lufthansa", resultSet.getString(2).equals("Lufthansa"));
                                Assert.assertTrue("Third Column should have value D", resultSet.getString(3).startsWith("D"));
                                break;
                            case 9:
                                Assert.assertTrue("First Column should have value NH", resultSet.getString(1).equals("NH"));
                                Assert.assertTrue("Second Column should have value Lufthansa", resultSet.getString(2).equals("All Nippon Airways"));
                                Assert.assertTrue("Third Column should have value D", resultSet.getString(3).startsWith("J"));
                                break;
                            case 10:
                                Assert.assertTrue("First Column should have value NH", resultSet.getString(1).equals("UA"));
                                Assert.assertTrue("Second Column should have value Lufthansa", resultSet.getString(2).equals("United Airlines"));
                                Assert.assertTrue("Third Column should have value D", resultSet.getString(3).startsWith("USA"));
                                break;
                        }                        
                        i++;
                    }
                }            
                break;
            default:
                break;
        }
    }
    
    private void checkSelectOr(SelectNoGroupStatement select, FedResultSet resultSet) throws FedException
    {
        int i = 0;
        switch(select.getTableIdentifiers().get(0).getIdentifier())
        {
            case "PASSAGIER":
                Assert.assertTrue("SELECT should have 3 columns.", resultSet.getColumnCount() == 3);
                while(resultSet.next())
                {
                    if(i == 0)
                    {
                        Assert.assertTrue("First Column should have value 9", resultSet.getString(1).equals("9"));
                        Assert.assertTrue("Second Column should have value Nannini", resultSet.getString(2).equals("Nannini"));
                        Assert.assertTrue("Third Column should have value AF", resultSet.getString(3).startsWith("I")); 
                    }
                    else{
                        Assert.assertTrue("First Column should have value 15", resultSet.getString(1).equals("15"));
                        Assert.assertTrue("Second Column should have value Ramazotti", resultSet.getString(2).equals("Ramazotti"));
                        Assert.assertTrue("Third Column should have value I", resultSet.getString(3).startsWith("I")); 
                    }
                    
                    i++;
                }
                
                break;
            case "BUCHUNG":
                BinaryExpression rightBinaryExpression = (BinaryExpression) (select.getWhereClause().getBinaryExpression().getRightOperand());
                switch(rightBinaryExpression.getOperator())
                {
                    case EQ:
                        FullyQualifiedAttributeIdentifier fq = (FullyQualifiedAttributeIdentifier)(rightBinaryExpression.getLeftOperand());
                        if(fq.getAttributeIdentifier().getIdentifier().equals("PNR"))
                        {
                            Assert.assertTrue("SELECT should have 4 columns.", resultSet.getColumnCount() == 4);
                            while(resultSet.next())
                            {
                                switch(i){
                                    case 0:
                                        Assert.assertTrue("First Column should have value 7", resultSet.getString(1).equals("7"));
                                        Assert.assertTrue("Second Column should have value 78", resultSet.getString(2).equals("78"));
                                        Assert.assertTrue("Third Column should have value DB", resultSet.getString(3).startsWith("DB"));
                                        Assert.assertTrue("Forth Column should have value 14", resultSet.getString(4).equals("14"));
                                        break;
                                    case 1:
                                        Assert.assertTrue("First Column should have value 6", resultSet.getString(1).equals("6"));
                                        Assert.assertTrue("Second Column should have value 78", resultSet.getString(2).equals("78"));
                                        Assert.assertTrue("Third Column should have value DB", resultSet.getString(3).startsWith("DB"));
                                        Assert.assertTrue("Forth Column should have value 15", resultSet.getString(4).equals("15"));
                                        break;
                                    case 2:
                                        Assert.assertTrue("First Column should have value 10", resultSet.getString(1).equals("10"));
                                        Assert.assertTrue("Second Column should have value 78", resultSet.getString(2).equals("78"));
                                        Assert.assertTrue("Third Column should have value DB", resultSet.getString(3).startsWith("DB"));
                                        Assert.assertTrue("Forth Column should have value 15", resultSet.getString(4).equals("15"));
                                        break;
                                    case 3:
                                        Assert.assertTrue("First Column should have value 8", resultSet.getString(1).equals("8"));
                                        Assert.assertTrue("Second Column should have value 78", resultSet.getString(2).equals("78"));
                                        Assert.assertTrue("Third Column should have value LH", resultSet.getString(3).startsWith("LH"));
                                        Assert.assertTrue("Forth Column should have value 50", resultSet.getString(4).equals("50"));
                                        break;
                                    case 4:
                                        Assert.assertTrue("First Column should have value 9", resultSet.getString(1).equals("9"));
                                        Assert.assertTrue("Second Column should have value 78", resultSet.getString(2).equals("78"));
                                        Assert.assertTrue("Third Column should have value LH",resultSet.getString(3).startsWith("LH"));
                                        Assert.assertTrue("Forth Column should have value 51", resultSet.getString(4).equals("51"));
                                        break;
                                }
                                i++;
                            }
                        }
                        else{
                            Assert.assertTrue("SELECT should have 9 columns.", resultSet.getColumnCount() == 9);
                            while(resultSet.next())
                            {
                                if(i == 0)
                                {
                                    Assert.assertTrue("First Column should have value 90", resultSet.getString(1).equals("90"));
                                    Assert.assertTrue("Second Column should have value 12", resultSet.getString(2).equals("12"));
                                    Assert.assertTrue("Third Column should have value AF", resultSet.getString(3).startsWith("AF"));
                                    Assert.assertTrue("Forth Column should have value 45", resultSet.getString(4).equals("45"));
                                    Assert.assertTrue("Fifth Column should have value CDG", resultSet.getString(5).equals("CDG"));
                                    Assert.assertTrue("Sixth Column should have value NRT", resultSet.getString(6).startsWith("NRT"));
                                    Assert.assertTrue("Seventh Column should have value 10-JAN-1998", resultSet.getString(7).equals("10-JAN-1998"));
                                    Assert.assertTrue("Eigth Column should have value 6200", resultSet.getString(8).equals("6200"));
                                    Assert.assertTrue("Nineth Column should have value 2500", resultSet.getString(9).startsWith("2500"));
                                }
                                else
                                {
                                    Assert.assertTrue("First Column should have value 2", resultSet.getString(1).equals("2"));
                                    Assert.assertTrue("Second Column should have value 74", resultSet.getString(2).equals("74"));
                                    Assert.assertTrue("Third Column should have value DB", resultSet.getString(3).startsWith("DB"));
                                    Assert.assertTrue("Forth Column should have value 62", resultSet.getString(4).equals("62"));
                                    Assert.assertTrue("Fifth Column should have value HKG", resultSet.getString(5).equals("HKG"));
                                    Assert.assertTrue("Sixth Column should have value FRA", resultSet.getString(6).startsWith("FRA"));
                                    Assert.assertTrue("Seventh Column should have value 25-APR-2011", resultSet.getString(7).equals("25-APR-2011"));
                                    Assert.assertTrue("Eigth Column should have value 250", resultSet.getString(8).equals("2000"));
                                    Assert.assertTrue("Nineth Column should have value 150", resultSet.getString(9).startsWith("150"));
                                }
                                i++;
                            }
                        }
                        break;
                    case GT:
                        Assert.assertTrue("SELECT should have 4 columns.", resultSet.getColumnCount() == 4);
                        Assert.assertTrue("ResultSet should be empty", !resultSet.next());
                        break;
                }
                break;
            case "FLUGHAFEN":
                Assert.assertTrue("SELECT should have 4 columns.", resultSet.getColumnCount() == 4);
                Assert.assertTrue("ResultSet should be empty", !resultSet.next());
            default:
                break;
        }
            
    }
    
    private void printResultSet(FedResultSet resultSet) throws FedException
    {
        while(resultSet.next())
        {
            for (int i = 1; i <= resultSet.getColumnCount(); i++) {
                System.out.printf("%-15s", resultSet.getString(i));
            }
            System.out.println();
        }
    }
}
