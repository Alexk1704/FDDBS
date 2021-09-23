package junit.fdbs.sql.parser;

import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.literal.StringLiteral;
import fdbs.sql.parser.ast.statement.UpdateStatement;
import fdbs.util.logger.ConsoleLogDestionation;
import fdbs.util.logger.Logger;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateStatementTest {

    public UpdateStatementTest() {
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
        Logger.infoln("Start to test parsing update statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing update statement.");
    }

    @Test
    public void testUpdateStatementNull() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = NULL");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("NULL", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof NullLiteral);

    }

    @Test
    public void testUpdateStatementString() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = 'Test132!'");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test132!", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof StringLiteral);

    }

    @Test
    public void testUpdateStatementInteger() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = 123");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("123", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof IntegerLiteral);
    }

    @Test
    public void testUpdateStatementNullWithWhereEQ() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = NULL where FLC = NULL");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("NULL", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof NullLiteral);

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("NULL", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.EQ.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof NullLiteral);

    }

    @Test
    public void testUpdateStatementStringWithWhereNEQ() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = 'Test132!' where FLC != 'Test132!'");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test132!", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof StringLiteral);

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("Test132!", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof StringLiteral);
    }

    @Test
    public void testUpdateStatementIntegerWithWhereGT() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = 123 where FLC > 123");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("123", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof IntegerLiteral);

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("123", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GT.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void testUpdateStatementIntegerWithWhereGEQ() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = 123 where FLC >= 123");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("123", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof IntegerLiteral);

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("123", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GEQ.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void testUpdateStatementIntegerWithWhereLT() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = 123 where FLC < 123");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("123", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof IntegerLiteral);

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("123", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LT.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void testUpdateStatementIntegerWithWhereLEQ() throws ParseException {
        SqlParser parser = new SqlParser("update FLUGLINIE set HUB = 123 where FLC <= 123");
        AST ast = parser.parseStatement();
        UpdateStatement stmt = (UpdateStatement) ast.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());
        Assert.assertEquals("HUB", stmt.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("123", stmt.getConstant().getIdentifier());
        Assert.assertTrue(stmt.getConstant() instanceof IntegerLiteral);

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals("FLC", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("123", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof IntegerLiteral);
    }
}
