package junit.fdbs.sql.sematic;

import fdbs.sql.FedException;
import fdbs.util.logger.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    junit.fdbs.sql.sematic.CreateTableStatementTest.class,
    junit.fdbs.sql.sematic.DropTableStatementTest.class,
    junit.fdbs.sql.sematic.DeleteStatementTest.class,
    junit.fdbs.sql.sematic.InsertStatementTest.class,
    junit.fdbs.sql.sematic.UpdateStatementTest.class,
    junit.fdbs.sql.sematic.SelectCountAllTableStatementTest.class,
    junit.fdbs.sql.sematic.SelectNoGroupStatementTest.class,
    junit.fdbs.sql.sematic.SelectGroupStatementTest.class,
    junit.fdbs.sql.sematic.TestFederatedTestAndValidationStatementsForDropTable.class
})
public class SemanticValidatorTestSuite {

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test semantic validators.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test semantic validators.");
    }
}
