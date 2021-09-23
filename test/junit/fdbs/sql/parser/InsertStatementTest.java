package junit.fdbs.sql.parser;

import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.literal.Literal;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.literal.StringLiteral;
import fdbs.sql.parser.ast.statement.InsertStatement;
import fdbs.util.logger.Logger;
import java.util.List;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class InsertStatementTest {

    public InsertStatementTest() {
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
        Logger.infoln("Start to test parsing insert statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing insert statement.");
    }

    @Test
    public void testInsertStatementWithOneValue() throws ParseException {
        SqlParser parser = new SqlParser("INSERT INTO FLUGLINIE VALUES (123)");
        AST ast = parser.parseStatement();
        InsertStatement stmt = (InsertStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        List<Literal> values = stmt.getValues();
        Assert.assertTrue(values.size() == 1);

        Literal value1 = values.get(0);
        Assert.assertEquals("123", value1.getIdentifier());
        Assert.assertTrue(value1 instanceof IntegerLiteral);
    }

    @Test
    public void testInsertStatementManyOneValue() throws ParseException {
        SqlParser parser = new SqlParser("INSERT INTO FLUGLINIE VALUES (123, 'USA1!', null, 'United Airlines', 'Star')");
        AST ast = parser.parseStatement();
        InsertStatement stmt = (InsertStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        List<Literal> values = stmt.getValues();
        Assert.assertTrue(values.size() == 5);

        Literal value1 = values.get(0);
        Literal value2 = values.get(1);
        Literal value3 = values.get(2);
        Literal value4 = values.get(3);
        Literal value5 = values.get(4);

        Assert.assertEquals("123", value1.getIdentifier());
        Assert.assertTrue(value1 instanceof IntegerLiteral);
        Assert.assertEquals("USA1!", value2.getIdentifier());
        Assert.assertTrue(value2 instanceof StringLiteral);
        Assert.assertEquals("null", value3.getIdentifier());
        Assert.assertTrue(value3 instanceof NullLiteral);
        Assert.assertEquals("United Airlines", value4.getIdentifier());
        Assert.assertTrue(value4 instanceof StringLiteral);
        Assert.assertEquals("Star", value5.getIdentifier());
        Assert.assertTrue(value5 instanceof StringLiteral);
    }
}
