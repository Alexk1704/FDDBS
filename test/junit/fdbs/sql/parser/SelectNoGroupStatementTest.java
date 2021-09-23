package junit.fdbs.sql.parser;

import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.identifier.FullyQualifiedAttributeIdentifier;
import fdbs.sql.parser.ast.identifier.TableIdentifier;
import fdbs.sql.parser.ast.statement.select.SelectNoGroupStatement;
import fdbs.util.logger.Logger;
import java.util.List;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SelectNoGroupStatementTest {

    public SelectNoGroupStatementTest() {
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
        Logger.infoln("Start to test parsing select no group statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing select no group statement.");
    }

    @Test
    public void testSelectNoGroupStatementWildCard() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM FLUGLINIE");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 1);
        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifiers().get(0).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());
        Assert.assertTrue(stmt.getWhereClause() == null);
    }

    @Test
    public void testSelectNoGroupStatementAttributesTwoTablesJoinWhere() throws ParseException {
        SqlParser parser = new SqlParser("select F.ID, F.Name FROM F, O WHERE (F.ID <= O.ID)");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() == null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().size() == 2);

        FullyQualifiedAttributeIdentifier attr1 = stmt.getFQAttributeIdentifiers().get(0);
        FullyQualifiedAttributeIdentifier attr2 = stmt.getFQAttributeIdentifiers().get(1);

        Assert.assertEquals("F", attr1.getIdentifier());
        Assert.assertEquals("ID", attr1.getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("F", attr2.getIdentifier());
        Assert.assertEquals("Name", attr2.getAttributeIdentifier().getIdentifier());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), binaryExpression.getOperatorName());
        Assert.assertEquals("F", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpression.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("O", binaryExpression.getRightOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpression.getRightOperand()).getAttributeIdentifier().getIdentifier());
    }

    @Test
    public void testSelectNoGroupStatementWildCardTwoTablesJoinAndNonJoinWhere() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM F, O WHERE (F.ID <= O.ID) AND ((F.ID = 1))");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());;

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.AND.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getLeftOperand() instanceof BinaryExpression);
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof BinaryExpression);

        BinaryExpression binaryExpressionLeft = (BinaryExpression) binaryExpression.getLeftOperand();
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), binaryExpressionLeft.getOperatorName());
        Assert.assertEquals("F", binaryExpressionLeft.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionLeft.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("O", binaryExpressionLeft.getRightOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionLeft.getRightOperand()).getAttributeIdentifier().getIdentifier());

        BinaryExpression binaryExpressionRight = (BinaryExpression) binaryExpression.getRightOperand();
        Assert.assertEquals(BinaryExpression.Operator.EQ.name(), binaryExpressionRight.getOperatorName());
        Assert.assertEquals("F", binaryExpressionRight.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionRight.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("1", binaryExpressionRight.getRightOperand().getIdentifier());
    }

    @Test
    public void testSelectNoGroupStatementWildCardTwoTablesNonJoinAndNonJoinWhere1() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM F, O WHERE ((F.ID < 1) AND (F.ID <= 1))");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.AND.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getLeftOperand() instanceof BinaryExpression);
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof BinaryExpression);

        BinaryExpression binaryExpressionLeft = (BinaryExpression) binaryExpression.getLeftOperand();
        Assert.assertEquals(BinaryExpression.Operator.LT.name(), binaryExpressionLeft.getOperatorName());
        Assert.assertEquals("F", binaryExpressionLeft.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionLeft.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("1", binaryExpressionLeft.getRightOperand().getIdentifier());

        BinaryExpression binaryExpressionRight = (BinaryExpression) binaryExpression.getRightOperand();
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), binaryExpressionRight.getOperatorName());
        Assert.assertEquals("F", binaryExpressionRight.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionRight.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("1", binaryExpressionRight.getRightOperand().getIdentifier());
    }

    @Test
    public void testSelectNoGroupStatementWildCardTwoTablesNonJoinAndNonJoinWhere2() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM F, O WHERE ((F.ID > 1) AND (F.ID >= 1))");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.AND.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getLeftOperand() instanceof BinaryExpression);
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof BinaryExpression);

        BinaryExpression binaryExpressionLeft = (BinaryExpression) binaryExpression.getLeftOperand();
        Assert.assertEquals(BinaryExpression.Operator.GT.name(), binaryExpressionLeft.getOperatorName());
        Assert.assertEquals("F", binaryExpressionLeft.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionLeft.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("1", binaryExpressionLeft.getRightOperand().getIdentifier());

        BinaryExpression binaryExpressionRight = (BinaryExpression) binaryExpression.getRightOperand();
        Assert.assertEquals(BinaryExpression.Operator.GEQ.name(), binaryExpressionRight.getOperatorName());
        Assert.assertEquals("F", binaryExpressionRight.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionRight.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("1", binaryExpressionRight.getRightOperand().getIdentifier());
    }

    @Test
    public void testSelectNoGroupStatementWildCardTwoTablesNonJoinAndNonJoinWhere3() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM F, O WHERE ((F.ID != 1) AND (F.ID = 'Test123!'))");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.AND.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getLeftOperand() instanceof BinaryExpression);
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof BinaryExpression);

        BinaryExpression binaryExpressionLeft = (BinaryExpression) binaryExpression.getLeftOperand();
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), binaryExpressionLeft.getOperatorName());
        Assert.assertEquals("F", binaryExpressionLeft.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionLeft.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("1", binaryExpressionLeft.getRightOperand().getIdentifier());

        BinaryExpression binaryExpressionRight = (BinaryExpression) binaryExpression.getRightOperand();
        Assert.assertEquals(BinaryExpression.Operator.EQ.name(), binaryExpressionRight.getOperatorName());
        Assert.assertEquals("F", binaryExpressionRight.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionRight.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test123!", binaryExpressionRight.getRightOperand().getIdentifier());
    }

    @Test
    public void testSelectNoGroupStatementWildCardTwoTablesNonJoinAndNonJoinWhere4() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM F, O WHERE ((F.ID != 'Test123!') OR (F.ID < 'Test123!'))");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.OR.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getLeftOperand() instanceof BinaryExpression);
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof BinaryExpression);

        BinaryExpression binaryExpressionLeft = (BinaryExpression) binaryExpression.getLeftOperand();
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), binaryExpressionLeft.getOperatorName());
        Assert.assertEquals("F", binaryExpressionLeft.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionLeft.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test123!", binaryExpressionLeft.getRightOperand().getIdentifier());

        BinaryExpression binaryExpressionRight = (BinaryExpression) binaryExpression.getRightOperand();
        Assert.assertEquals(BinaryExpression.Operator.LT.name(), binaryExpressionRight.getOperatorName());
        Assert.assertEquals("F", binaryExpressionRight.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionRight.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test123!", binaryExpressionRight.getRightOperand().getIdentifier());
    }

    @Test
    public void testSelectNoGroupStatementWildCardTwoTablesNonJoinAndNonJoinWhere5() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM F, O WHERE ((F.ID <= 'Test123!') AND (F.ID > 'Test123!'))");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.AND.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getLeftOperand() instanceof BinaryExpression);
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof BinaryExpression);

        BinaryExpression binaryExpressionLeft = (BinaryExpression) binaryExpression.getLeftOperand();
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), binaryExpressionLeft.getOperatorName());
        Assert.assertEquals("F", binaryExpressionLeft.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionLeft.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test123!", binaryExpressionLeft.getRightOperand().getIdentifier());

        BinaryExpression binaryExpressionRight = (BinaryExpression) binaryExpression.getRightOperand();
        Assert.assertEquals(BinaryExpression.Operator.GT.name(), binaryExpressionRight.getOperatorName());
        Assert.assertEquals("F", binaryExpressionRight.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionRight.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test123!", binaryExpressionRight.getRightOperand().getIdentifier());
    }

    @Test
    public void testSelectNoGroupStatementWildCardTwoTablesNonJoinAndNonJoinWhere6() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM F, O WHERE ((F.ID >= 'Test123!') OR (F.ID > 'Test123!'))");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.OR.name(), binaryExpression.getOperatorName());
        Assert.assertTrue(binaryExpression.getLeftOperand() instanceof BinaryExpression);
        Assert.assertTrue(binaryExpression.getRightOperand() instanceof BinaryExpression);

        BinaryExpression binaryExpressionLeft = (BinaryExpression) binaryExpression.getLeftOperand();
        Assert.assertEquals(BinaryExpression.Operator.GEQ.name(), binaryExpressionLeft.getOperatorName());
        Assert.assertEquals("F", binaryExpressionLeft.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionLeft.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test123!", binaryExpressionLeft.getRightOperand().getIdentifier());

        BinaryExpression binaryExpressionRight = (BinaryExpression) binaryExpression.getRightOperand();
        Assert.assertEquals(BinaryExpression.Operator.GT.name(), binaryExpressionRight.getOperatorName());
        Assert.assertEquals("F", binaryExpressionRight.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpressionRight.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test123!", binaryExpressionRight.getRightOperand().getIdentifier());
    }

    @Test
    public void testSelectNoGroupStatementWildCardTwoTablesNonJoinAndNonJoinWhere7() throws ParseException {
        SqlParser parser = new SqlParser("select * FROM F, O WHERE ((F.ID >= 'Test123!'))");
        AST ast = parser.parseStatement();

        SelectNoGroupStatement stmt = (SelectNoGroupStatement) ast.getRoot();

        List<TableIdentifier> tableIdentifiers = stmt.getTableIdentifiers();
        Assert.assertTrue(tableIdentifiers.size() == 2);
        Assert.assertEquals("F", stmt.getTableIdentifiers().get(0).getIdentifier());
        Assert.assertEquals("O", stmt.getTableIdentifiers().get(1).getIdentifier());

        Assert.assertTrue(stmt.getAllAttributeIdentifier() != null);
        Assert.assertTrue(stmt.getFQAttributeIdentifiers().isEmpty());

        BinaryExpression binaryExpression = stmt.getWhereClause().getBinaryExpression();
        Assert.assertEquals(BinaryExpression.Operator.GEQ.name(), binaryExpression.getOperatorName());

        Assert.assertEquals("F", binaryExpression.getLeftOperand().getIdentifier());
        Assert.assertEquals("ID", ((FullyQualifiedAttributeIdentifier) binaryExpression.getLeftOperand()).getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("Test123!", binaryExpression.getRightOperand().getIdentifier());
    }
}
