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
import fdbs.sql.resolver.CreateStatementResolver;
import fdbs.util.statement.StatementUtil;
import java.io.IOException;
import java.util.logging.Level;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author alex1704
 */
public class CreateStatementResolverTest {

    String[] statements;
    FedConnection fed;

    public CreateStatementResolverTest() {
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
            manager.deleteMetadata();
            manager.deleteTables();
            manager.checkAndRefreshTables(); 
            statements = StatementUtil.getStatementsFromFiles(new String[]{"src\\fdbs\\app\\federated\\test\\CREPARTABS_CUSTOM.sql"});
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
            CreateStatementResolver resolver = new CreateStatementResolver(fed);
            resolver.resolveStatement(tree);
            System.out.println("<-------------------------------------------------------------------->");
        }
       
        //SqlParser parser = new SqlParser("create table PASSAGIER (\n" +
        //"PNR		integer, " +
        //"NAME		varchar(40), " +
        //"VORNAME		varchar(40), " +
        //"LAND		varchar(3), " +
        //"constraint PASSAGIER_PS" +
        //"		primary key (PNR), " +
        //"constraint PASSAGIER_NAME_NN" +
        //"        check (NAME is not null), " +
        //"constraint PASSAGIER_UNIQUE UNIQUE(NAME), " +
        //"constraint PASSAGIER_VORNAME_NN" +
        //"        check (VORNAME is not null), " +   
        //"constraint FLUG_AN_CHK" +
        //"                    check (PNR between 0 and 2400), " +
        //"constraint FLUG_VONNACH_CHK" +
        //"		check (NAME != VORNAME), " +
        //"constraint FLUG_ABC_CHK" +
        //"		check (PNR <= 50), " +
        //"constraint LAND_UNIQUE UNIQUE(VORNAME) " +
        //") " +
        //"VERTICAL ((PNR, LAND, NAME),(VORNAME))");    
        //ast = parser.parseStatement();        
    }
}
