/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.FedPseudoDriver;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.resolver.DropStatementResolver;
import fdbs.util.statement.StatementUtil;
import java.io.IOException;
import java.util.logging.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author alex1704
 */
public class DropStatementResolverTest {

    String[] statements;
    FedConnection fed;

    public DropStatementResolverTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        try {
            fed = (new FedPseudoDriver()).getConnection("VDBSB01", "VDBSB01");
            MetadataManager manager = MetadataManager.getInstance(fed);
            manager.checkAndRefreshTables();
            statements = StatementUtil.getStatementsFromFiles(new String[]{"src\\fdbs\\app\\federated\\test\\DRPTABS_CUSTOM.sql"});
        } catch (FedException | IOException ex) { 
            java.util.logging.Logger.getLogger(CreateStatementResolverTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testResolveStatement() throws Exception {
        AST tree = null;
        for (String stm : statements) {
            SqlParser parser = new SqlParser(stm);
            tree = parser.parseStatement();
            System.out.println("Statement:" + stm);
            DropStatementResolver resolver = new DropStatementResolver(fed);
            resolver.resolveStatement(tree);
            System.out.println("<-------------------------------------------------------------------->");
        }
    }
}
