package junit.fdbs.sql.fedresultset.select;

import fdbs.sql.FedException;
import fdbs.util.logger.Logger;
import junit.fdbs.sql.fedresultset.select.AllAttributes.AllAtributesGroupResultSetTest;
import junit.fdbs.sql.fedresultset.select.AllAttributes.AllAttributeNoGroupWithNonJoinAndNonJoinResultSetTest;
import junit.fdbs.sql.fedresultset.select.AllAttributes.AllAttributeNoGroupWithNonJoinOrNonJoinResultSetTest;
import junit.fdbs.sql.fedresultset.select.AllAttributes.AllAttributeNoGroupWithNonJoinResultSetTest;
import junit.fdbs.sql.fedresultset.select.AllAttributes.AllAttributeNoGrouptResultSetTest;
import junit.fdbs.sql.fedresultset.select.AllAttributes.GroupByTest;
import junit.fdbs.sql.fedresultset.select.AllAttributes.SelectCountAllTableStatementResultSetTest;
import junit.fdbs.sql.fedresultset.select.Attributes.MetaDataTests.AttributesNoGroupResultSetMetaDataTest;
import junit.fdbs.sql.fedresultset.select.Attributes.MetaDataTests.AttributesNoGroupWithNonJoinAndNonJoinResultSetMetaDataTest;
import junit.fdbs.sql.fedresultset.select.Attributes.MetaDataTests.AttributesNoGroupWithNonJoinOrNonJoinResultSetMetaDataTest;
import junit.fdbs.sql.fedresultset.select.Attributes.MetaDataTests.AttributesNoGroupWithNonJoinResultSetMetaDataTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
//    SelectCountAllTableStatementResultSetTest.class,
//    AllAttributeNoGrouptResultSetTest.class,
//    AttributesNoGroupResultSetMetaDataTest.class,
//    AllAttributeNoGroupWithNonJoinResultSetTest.class,
//    AttributesNoGroupWithNonJoinResultSetMetaDataTest.class,
//    AllAttributeNoGroupWithNonJoinAndNonJoinResultSetTest.class,
//    AttributesNoGroupWithNonJoinAndNonJoinResultSetMetaDataTest.class,
//    AllAttributeNoGroupWithNonJoinOrNonJoinResultSetTest.class,
//    AttributesNoGroupWithNonJoinOrNonJoinResultSetMetaDataTest.class,
        GroupByTest.class
})

public class SelectFedResultTestSuite {

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws FedException {
        Logger.getLogger();
        Logger.infoln("Start to test result sets.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test result sets.");
    }
}
