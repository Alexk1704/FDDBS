package junit.fdbs.sql.meta;

import fdbs.util.statement.StatementUtil;
import fdbs.sql.meta.MetadataManager;
import junit.framework.TestCase;
import org.junit.*;
import fdbs.util.logger.*;
import fdbs.sql.*;
import fdbs.sql.parser.*;
import fdbs.sql.parser.ast.*;
import java.io.*;
import java.util.logging.Level;

public class ManagerTestAST extends TestCase {

    public ManagerTestAST() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        System.out.println("Start to test Manager class:");
    }

    @After
    public void tearDown() {
        System.out.println("End of Manager class test.");
    }

    @Test
    public void testManagerAST() throws FedException {
        try {
            ManagerTestUtils.SetUpDatabase("VDBSA01", "VDBSA01");
        } catch (ParseException ex) {
            Logger.error(ex.toString());
            java.util.logging.Logger.getLogger(ManagerTestAST.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
