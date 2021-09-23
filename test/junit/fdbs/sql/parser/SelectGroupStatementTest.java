package junit.fdbs.sql.parser;

import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.function.CountFunction;
import fdbs.sql.parser.ast.function.MaxFunction;
import fdbs.sql.parser.ast.function.SumFunction;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.identifier.TableIdentifier;
import fdbs.sql.parser.ast.literal.IntegerLiteral;
import fdbs.sql.parser.ast.statement.select.SelectGroupStatement;
import fdbs.util.logger.Logger;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SelectGroupStatementTest {

    public SelectGroupStatementTest() {
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
        Logger.infoln("Start to test parsing select group statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing select group statement.");
    }

    @Test
    public void SelectGroupStatementAttributGroupBy() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, max(F.ID) FROM F GROUP BY F.ID");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 1);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("F", ((MaxFunction) stmt.getFunction()).getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", ((MaxFunction) stmt.getFunction()).getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertTrue(stmt.getWhereClause() == null);
        Assert.assertTrue(stmt.getHavingClause() == null);

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());
    }

    @Test
    public void SelectGroupStatementAttributFunctionGroupByHavingEQInteger() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, count(*) FROM F,O GROUP BY F.ID HAVING (COUNT(*) = 1)");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertTrue(stmt.getFunction() instanceof CountFunction);

        Assert.assertTrue(stmt.getWhereClause() == null);

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression countComparisonExpression = stmt.getHavingClause().getCountComparisonExpression();
        Assert.assertTrue(countComparisonExpression.getLeftOperand() instanceof CountFunction);
        Assert.assertEquals("1", countComparisonExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.EQ.name(), countComparisonExpression.getOperatorName());
        Assert.assertTrue(countComparisonExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void SelectGroupStatementAttributFunctionGroupByHavingNEQInteger() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, count(*) FROM F,O GROUP BY F.ID HAVING (COUNT(*) != 1)");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertTrue(stmt.getFunction() instanceof CountFunction);

        Assert.assertTrue(stmt.getWhereClause() == null);

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression countComparisonExpression = stmt.getHavingClause().getCountComparisonExpression();
        Assert.assertTrue(countComparisonExpression.getLeftOperand() instanceof CountFunction);
        Assert.assertEquals("1", countComparisonExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), countComparisonExpression.getOperatorName());
        Assert.assertTrue(countComparisonExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void SelectGroupStatementAttributFunctionGroupByHavingGTInteger() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, count(*) FROM F,O GROUP BY F.ID HAVING (COUNT(*) > 1)");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertTrue(stmt.getFunction() instanceof CountFunction);

        Assert.assertTrue(stmt.getWhereClause() == null);

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression countComparisonExpression = stmt.getHavingClause().getCountComparisonExpression();
        Assert.assertTrue(countComparisonExpression.getLeftOperand() instanceof CountFunction);
        Assert.assertEquals("1", countComparisonExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GT.name(), countComparisonExpression.getOperatorName());
        Assert.assertTrue(countComparisonExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void SelectGroupStatementAttributFunctionGroupByHavingGEQInteger() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, count(*) FROM F,O GROUP BY F.ID HAVING (COUNT(*) >= 1)");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertTrue(stmt.getFunction() instanceof CountFunction);

        Assert.assertTrue(stmt.getWhereClause() == null);

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression countComparisonExpression = stmt.getHavingClause().getCountComparisonExpression();
        Assert.assertTrue(countComparisonExpression.getLeftOperand() instanceof CountFunction);
        Assert.assertEquals("1", countComparisonExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GEQ.name(), countComparisonExpression.getOperatorName());
        Assert.assertTrue(countComparisonExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void SelectGroupStatementAttributFunctionGroupByHavingLTInteger() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, count(*) FROM F,O GROUP BY F.ID HAVING (COUNT(*) < 1)");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertTrue(stmt.getFunction() instanceof CountFunction);

        Assert.assertTrue(stmt.getWhereClause() == null);

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression countComparisonExpression = stmt.getHavingClause().getCountComparisonExpression();
        Assert.assertTrue(countComparisonExpression.getLeftOperand() instanceof CountFunction);
        Assert.assertEquals("1", countComparisonExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LT.name(), countComparisonExpression.getOperatorName());
        Assert.assertTrue(countComparisonExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void SelectGroupStatementAttributFunctionGroupByHavingLEQInteger() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, count(*) FROM F,O GROUP BY F.ID HAVING (COUNT(*) <= 1)");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertTrue(stmt.getFunction() instanceof CountFunction);

        Assert.assertTrue(stmt.getWhereClause() == null);

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression countComparisonExpression = stmt.getHavingClause().getCountComparisonExpression();
        Assert.assertTrue(countComparisonExpression.getLeftOperand() instanceof CountFunction);
        Assert.assertEquals("1", countComparisonExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), countComparisonExpression.getOperatorName());
        Assert.assertTrue(countComparisonExpression.getRightOperand() instanceof IntegerLiteral);
    }

    @Test
    public void SelectGroupStatementAttributWhereGroupBy() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, sum(F.ID) FROM F, O WHERE (F.ID <= O.ID) GROUP BY F.ID");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("F", ((SumFunction) stmt.getFunction()).getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", ((SumFunction) stmt.getFunction()).getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        junit.framework.Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), binaryExpression.getOperatorName());
        junit.framework.Assert.assertEquals("F", binaryExpression.getLeftOperand().getIdentifier());
        junit.framework.Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpression.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        junit.framework.Assert.assertEquals("O", binaryExpression.getRightOperand().getIdentifier());
        junit.framework.Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpression.getRightOperand()).getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertTrue(stmt.getHavingClause() == null);

    }

    @Test
    public void SelectGroupStatementAttributWhereGroupByHaving() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, sum(F.ID) FROM F, O WHERE (F.ID <= O.ID) GROUP BY F.ID HAVING (COUNT(*) <= 1)");
        AST ast = parser.parseStatement();

        SelectGroupStatement stmt = (SelectGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertEquals("F", stmt.getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("F", ((SumFunction) stmt.getFunction()).getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", ((SumFunction) stmt.getFunction()).getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        junit.framework.Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), binaryExpression.getOperatorName());
        junit.framework.Assert.assertEquals("F", binaryExpression.getLeftOperand().getIdentifier());
        junit.framework.Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpression.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        junit.framework.Assert.assertEquals("O", binaryExpression.getRightOperand().getIdentifier());
        junit.framework.Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpression.getRightOperand()).getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("F", stmt.getGroupByClause().getFQAttributeIdentifier().getIdentifier());
        Assert.assertEquals("ID", stmt.getGroupByClause().getFQAttributeIdentifier().getAttributeIdentifier().getIdentifier());

        BinaryExpression countComparisonExpression = stmt.getHavingClause().getCountComparisonExpression();
        Assert.assertTrue(countComparisonExpression.getLeftOperand() instanceof CountFunction);
        Assert.assertEquals("1", countComparisonExpression.getRightOperand().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), countComparisonExpression.getOperatorName());
        Assert.assertTrue(countComparisonExpression.getRightOperand() instanceof IntegerLiteral);
    }
}
