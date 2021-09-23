package junit.fdbs.sql.parser;

import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.literal.StringLiteral;
import fdbs.sql.parser.ast.statement.DeleteStatement;
import fdbs.util.logger.Logger;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteStatementTest {

    public DeleteStatementTest() {
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
        Logger.infoln("Start to test parsing delete statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing delete statement.");
    }

    @Test
    public void testDeleteStatementNullWithWhereEQ() throws ParseException {
        SqlParser parser = new SqlParser("delete from FLUGLINIE where FLC = NULL");
        AST ast = parser.parseStatement();
        DeleteStatement stmt = (DeleteStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("NULL", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.EQ.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof NullLiteral);
    }

    @Test
    public void testDeleteStatementStringWithWhereNEQ() throws ParseException {
        SqlParser parser = new SqlParser("delete from FLUGLINIE where FLC != 'Test132!'");
        AST ast = parser.parseStatement();
        DeleteStatement stmt = (DeleteStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("Test132!", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof StringLiteral);
    }

    @Test
    public void testDeleteStatementIntegerWithWhereGT() throws ParseException {
        SqlParser parser = new SqlParser("delete from FLUGLINIE where FLC > 123");
        AST ast = parser.parseStatement();
        DeleteStatement stmt = (DeleteStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("123", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GT.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void testDeleteStatementIntegerWithWhereGEQ() throws ParseException {
        SqlParser parser = new SqlParser("delete from FLUGLINIE where FLC >= 123");
        AST ast = parser.parseStatement();
        DeleteStatement stmt = (DeleteStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("123", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GEQ.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void testDeleteStatementIntegerWithWhereLT() throws ParseException {
        SqlParser parser = new SqlParser("delete from FLUGLINIE where FLC < 123");
        AST ast = parser.parseStatement();
        DeleteStatement stmt = (DeleteStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("123", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LT.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void testDeleteStatementIntegerWithWhereLEQ() throws ParseException {
        SqlParser parser = new SqlParser("delete from FLUGLINIE where FLC <= 123");
        AST ast = parser.parseStatement();
        DeleteStatement stmt = (DeleteStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("123", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof IntegerLiteral);
    }
}
