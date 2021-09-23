package junit.fdbs.sql.parser;

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
    junit.fdbs.sql.parser.CreateTableStatementTest.class,
    junit.fdbs.sql.parser.DropTableStatementTest.class,
    junit.fdbs.sql.parser.DeleteStatementTest.class,
    junit.fdbs.sql.parser.InsertStatementTest.class,
    junit.fdbs.sql.parser.InvalidStatementTests.class,
    junit.fdbs.sql.parser.UpdateStatementTest.class,
    junit.fdbs.sql.parser.SelectCountAllTableStatementTest.class,
    junit.fdbs.sql.parser.SelectGroupStatementTest.class,
    junit.fdbs.sql.parser.SelectNoGroupStatementTest.class,
    junit.fdbs.sql.parser.TestFederatedTestAndValidationStatements.class
})

public class ParserTestSuite {

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test parser and abstract syntax tree.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parser and abstract syntax tree.");
    }
}
