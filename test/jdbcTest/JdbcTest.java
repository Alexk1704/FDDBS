/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jdbcTest;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.semantic.SemanticValidator;
import fdbs.util.logger.Logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Patrick
 */
public class JdbcTest {

    private static FedConnection fedConnection;
    private static MetadataManager metadataManager;

    public JdbcTest() {
    }

    @BeforeClass
    public static void setUpClass() throws FedException {
        fedConnection = new FedConnection("VDBSA01", "VDBSA01");
        metadataManager = MetadataManager.getInstance(fedConnection);
    }

    @AfterClass
    public static void tearDownClass() throws FedException {
        fedConnection.close();
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test create table statement semantic validator.");

        try {
            metadataManager.deleteTables();
        } catch (FedException ex) {
        }

        try {
            metadataManager.checkAndRefreshTables();
        } catch (FedException ex) {
        }

        try {
            executeStatement("drop table FLUGLINIE", 1);
        } catch (SQLException | FedException ex) {
        }
    }

    @After
    public void tearDown() {
        try {
            metadataManager.deleteTables();
        } catch (FedException ex) {
        }

        Logger.infoln("Finished to test create table statement semantic validator.");
    }

    @Test
    public void test() throws FedException, SQLException, Exception {
        String stmt = String.format("SELECT * FROM %s", "FLUGLINIE");

        ResultSet resultSet = null;
        Connection dbConnection = fedConnection.getConnections().get(1);

        try {
            java.sql.Statement createStatement = dbConnection.createStatement();
            int result = createStatement.executeUpdate("create table FLUGLINIE ( FLC varchar(2), Test integer, Test2 integer, constraint FLUGLINIE_FLC primary key (FLC))");
            java.sql.Statement insertStatement = dbConnection.createStatement();
            int resultInsert = insertStatement.executeUpdate("INSERT INTO FLUGLINIE VALUES ('AB', 1, 2)");
            java.sql.Statement selectStatement = dbConnection.createStatement();
            resultSet = selectStatement.executeQuery(stmt);
            
            assertEquals(0, result);
            assertEquals(1, resultInsert);
            
            ResultSetMetaData metaData = resultSet.getMetaData();
            assertEquals(true, resultSet.next());
            resultSet.next();
            System.out.println("HALLO LOG MAL");
            assertEquals(false, resultSet.next());
            assertEquals( "AB", resultSet.getString(1));
            assertEquals("1", resultSet.getString(2));
            assertEquals("2", resultSet.getString(3));
            String value = resultSet.getString(1);
            System.out.println(value);
            assertEquals("AB", resultSet.getInt(1));

            Logger.logln(resultSet.getString(1));
            Logger.logln("GetInt(1) : " + resultSet.getInt(1));
            Logger.logln("GetInt(1) : " + resultSet.getInt(2));
            Logger.logln(resultSet.getString(2));
            Logger.logln(resultSet.getString(3));

            Logger.logln("getColumnCount() : " + metaData.getColumnCount());
            Logger.logln("getColumnType(1) : " + metaData.getColumnType(1));
            Logger.logln("getColumnType(2) : " + metaData.getColumnType(2));
            Logger.logln("getColumnType(3) : " + metaData.getColumnType(3));

            Logger.logln(metaData.getColumnName(1));
            Logger.logln(metaData.getColumnName(2));

            String i = "";
        } catch (SQLException ex) {
        }

        executeStatement("drop table FLUGLINIE", 1);
    }

    // <editor-fold defaultstate="collapsed" desc="Private Test Helper Functions" >
    private void parseValidateHandleStatement(String statement) throws ParseException, Exception {
        handleStatement(validateAST(parseStatement(statement)));
    }

    private void parseValidateStatement(String statement) throws ParseException, Exception {
        validateAST(parseStatement(statement));
    }

    private AST parseStatement(String statement) throws ParseException {
        SqlParser parserCreateDB = new SqlParser(statement);
        return parserCreateDB.parseStatement();
    }

    private AST validateAST(AST ast) throws FedException, Exception {
        ast.getRoot().accept(new SemanticValidator(fedConnection));
        return ast;
    }

    private void handleStatement(AST ast) throws FedException {
        metadataManager.handleQuery(ast);
    }

    private void executeStatement(String stmt, Integer db) throws FedException, SQLException {
        SQLExecuter sqlExecuter = new SQLExecuter(fedConnection);
        SQLExecuterTask task = new SQLExecuterTask();
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
        task.close();
    }
    // </editor-fold>
}
