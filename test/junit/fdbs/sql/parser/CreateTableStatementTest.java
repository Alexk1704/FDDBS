package junit.fdbs.sql.parser;

import fdbs.sql.FedException;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.attribute.TableAttribute;
import fdbs.sql.parser.ast.clause.HorizontalClause;
import fdbs.sql.parser.ast.constraint.*;
import fdbs.sql.parser.ast.expression.*;
import fdbs.sql.parser.ast.identifier.AttributeIdentifier;
import fdbs.sql.parser.ast.literal.StringLiteral;
import fdbs.sql.parser.ast.statement.CreateTableStatement;
import fdbs.sql.parser.ast.type.PrimitiveType;
import fdbs.sql.parser.ast.type.VarCharType;
import fdbs.util.logger.Logger;
import java.util.List;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
//import sun.util.logging.PlatformLogger;

public class CreateTableStatementTest {

    public CreateTableStatementTest() throws ParseException {

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
        Logger.infoln("Start to test parsing create table statement.");
    }

    @After
    public void tearDown() {
        Logger.infoln("Finished to test parsing create table statement.");
    }

    @Test
    public void testSimpleVerticalCreateStatement() throws ParseException {
        SqlParser parser = new SqlParser("create table FLUGHAFEN (FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC) ) VERTICAL ((FHC, LAND), (STADT, NAME))");
        AST ast1 = parser.parseStatement();
        CreateTableStatement stmt = (CreateTableStatement) ast1.getRoot();
        Assert.assertEquals("FLUGHAFEN", stmt.getTableIdentifier().getIdentifier());

        List<TableAttribute> attributes = stmt.getAttributes();
        Assert.assertTrue(attributes.size() == 4);
        TableAttribute fhc = attributes.get(0);
        TableAttribute land = attributes.get(1);
        TableAttribute stadt = attributes.get(2);
        TableAttribute name = attributes.get(3);

        Assert.assertEquals("FHC", fhc.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) fhc.getType()).getLength().getIdentifier().equals("3"));
        Assert.assertEquals("LAND", land.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) land.getType()).getLength().getIdentifier().equals("3"));
        Assert.assertEquals("STADT", stadt.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) stadt.getType()).getLength().getIdentifier().equals("50"));
        Assert.assertEquals("NAME", name.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) name.getType()).getLength().getIdentifier().equals("50"));

        List<Constraint> constraints = stmt.getConstraints();
        Assert.assertTrue(constraints.size() == 1);
        PrimaryKeyConstraint primConstraint = (PrimaryKeyConstraint) constraints.get(0);
        Assert.assertEquals("FLUGHAFEN_PS", primConstraint.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("FHC", primConstraint.getAttributeIdentifier().getIdentifier());

        List<AttributeIdentifier> attributesForDB1 = stmt.getVerticalClause().getAttributesForDB1();
        Assert.assertEquals("FHC", attributesForDB1.get(0).getIdentifier());
        Assert.assertEquals("LAND", attributesForDB1.get(1).getIdentifier());

        List<AttributeIdentifier> attributesForDB2 = stmt.getVerticalClause().getAttributesForDB2();
        Assert.assertEquals("STADT", attributesForDB2.get(0).getIdentifier());
        Assert.assertEquals("NAME", attributesForDB2.get(1).getIdentifier());

        Assert.assertTrue(stmt.getVerticalClause().getAttributesForDB3().isEmpty());
    }

    @Test
    public void testHorizontalCreateStatementWithClauses() throws ParseException {
        SqlParser parser = new SqlParser("create table FLUGLINIE ( FLC varchar(2), LAND varchar(3) DEFAULT('DEU'), HUB varchar(3) DEFAULT(0), NAME varchar(30), ALLIANZ varchar(20), constraint FLUGLINIE_PS primary key (FLC), constraint FLUGLINIE_FS_HUB foreign key (HUB) references FLUGHAFEN(FHC), constraint FLUGLINIE_LAND_NN check (LAND is not null), constraint FLUGLINIE_ALLIANZ_CHK check (ALLIANZ != 'BlackList') ) HORIZONTAL (FLC('KK','MM'))");
        AST ast2 = parser.parseStatement();
        CreateTableStatement stmt = (CreateTableStatement) ast2.getRoot();

        Assert.assertEquals("FLUGLINIE", stmt.getTableIdentifier().getIdentifier());

        List<TableAttribute> attributes = stmt.getAttributes();
        Assert.assertTrue(attributes.size() == 5);
        TableAttribute flc = attributes.get(0);
        TableAttribute land = attributes.get(1);
        TableAttribute hub = attributes.get(2);
        TableAttribute name = attributes.get(3);
        TableAttribute allianz = attributes.get(4);

        Assert.assertEquals("FLC", flc.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) flc.getType()).getLength().getIdentifier().equals("2"));
        Assert.assertEquals("LAND", land.getIdentifierNode().getIdentifier());
        Assert.assertEquals("DEU", land.getDefaultValue().getIdentifier());
        Assert.assertTrue(land.getDefaultValue() instanceof StringLiteral);
        Assert.assertTrue(((VarCharType) land.getType()).getLength().getIdentifier().equals("3"));
        Assert.assertEquals("HUB", hub.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) hub.getType()).getLength().getIdentifier().equals("3"));
        Assert.assertEquals("0", hub.getDefaultValue().getIdentifier());
        Assert.assertEquals("NAME", name.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) name.getType()).getLength().getIdentifier().equals("30"));
        Assert.assertEquals("ALLIANZ", allianz.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) allianz.getType()).getLength().getIdentifier().equals("20"));

        List<Constraint> constraints = stmt.getConstraints();
        Assert.assertTrue(constraints.size() == 4);

        PrimaryKeyConstraint primConstraint = (PrimaryKeyConstraint) constraints.get(0);
        ForeignKeyConstraint foreignKeyConstraint = (ForeignKeyConstraint) constraints.get(1);
        CheckConstraint check1 = (CheckConstraint) constraints.get(2);
        CheckConstraint check2 = (CheckConstraint) constraints.get(3);

        Assert.assertEquals("FLUGLINIE_PS", primConstraint.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("FLC", primConstraint.getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("FLUGLINIE_FS_HUB", foreignKeyConstraint.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("HUB", foreignKeyConstraint.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("FHC", foreignKeyConstraint.getReferencedAttribute().getIdentifier());
        Assert.assertEquals("FLUGHAFEN", foreignKeyConstraint.getReferencedTable().getIdentifier());

        Assert.assertEquals("FLUGLINIE_LAND_NN", check1.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(UnaryExpression.Operator.ISNOTNULL.name(), ((UnaryExpression) check1.getCheckExpression()).getOperatorName());
        Assert.assertEquals("LAND", ((UnaryExpression) check1.getCheckExpression()).getOperand().getIdentifier());

        Assert.assertEquals("FLUGLINIE_ALLIANZ_CHK", check2.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), ((BinaryExpression) check2.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ALLIANZ", ((BinaryExpression) check2.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("BlackList", ((BinaryExpression) check2.getCheckExpression()).getRightOperand().getIdentifier());
        Assert.assertTrue(((BinaryExpression) check2.getCheckExpression()).getRightOperand() instanceof StringLiteral);

        HorizontalClause horizontalClause = stmt.getHorizontalClause();
        Assert.assertEquals("FLC", horizontalClause.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("KK", horizontalClause.getFirstBoundary().getLiteral().getIdentifier());
        Assert.assertEquals("MM", horizontalClause.getSecondBoundary().getLiteral().getIdentifier());
        Assert.assertTrue(horizontalClause.getFirstBoundary().getLiteral() instanceof StringLiteral);
        Assert.assertTrue(horizontalClause.getSecondBoundary().getLiteral() instanceof StringLiteral);
    }

    @Test
    public void testCreateStatementWithClauses() throws ParseException {
        SqlParser parser = new SqlParser("create table FLUG ( FNR integer, FLC varchar(2), FLNR integer, VON	varchar(3), NACH varchar(3), AB integer, AN integer, constraint FLUG_PS primary key (FNR), constraint FLUG_FS_FLC foreign key (FLC) references FLUGLINIE(FLC), constraint FLUG_FS_VON foreign key (VON) references FLUGHAFEN(FHC), constraint FLUG_FS_NACH foreign key (NACH) references FLUGHAFEN(FHC), constraint FLUG_VON_NN check (VON is not null), constraint FLUG_NACH_NN check (NACH is not null), constraint FLUG_AB_NN check (AB is not null), constraint FLUG_AN_NN check (AN is not null), constraint FLUG_AB_CHK check (AB between 0 and 2400), constraint FLUG_AN_CHK check (AN between 0 and 2400), constraint FLUG_VONNACH_CHK check (VON != NACH) ) HORIZONTAL (FLC('KK','MM'))");
        AST ast3 = parser.parseStatement();
        CreateTableStatement stmt = (CreateTableStatement) ast3.getRoot();

        Assert.assertEquals("FLUG", stmt.getTableIdentifier().getIdentifier());

        List<TableAttribute> attributes = stmt.getAttributes();
        Assert.assertTrue(attributes.size() == 7);
        TableAttribute fnr = attributes.get(0);
        TableAttribute flc = attributes.get(1);
        TableAttribute flnr = attributes.get(2);
        TableAttribute von = attributes.get(3);
        TableAttribute nach = attributes.get(4);
        TableAttribute ab = attributes.get(5);
        TableAttribute an = attributes.get(6);

        Assert.assertEquals("FNR", fnr.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((PrimitiveType) fnr.getType()).getPrimitive() == PrimitiveType.Primitive.INTEGER);
        Assert.assertEquals("FLC", flc.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) flc.getType()).getLength().getIdentifier().equals("2"));
        Assert.assertEquals("FLNR", flnr.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((PrimitiveType) flnr.getType()).getPrimitive() == PrimitiveType.Primitive.INTEGER);
        Assert.assertEquals("VON", von.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) von.getType()).getLength().getIdentifier().equals("3"));
        Assert.assertEquals("NACH", nach.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) nach.getType()).getLength().getIdentifier().equals("3"));
        Assert.assertEquals("AB", ab.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((PrimitiveType) ab.getType()).getPrimitive() == PrimitiveType.Primitive.INTEGER);
        Assert.assertEquals("AN", an.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((PrimitiveType) an.getType()).getPrimitive() == PrimitiveType.Primitive.INTEGER);

        List<Constraint> constraints = stmt.getConstraints();
        Assert.assertTrue(constraints.size() == 11);

        PrimaryKeyConstraint primConstraint = (PrimaryKeyConstraint) constraints.get(0);
        ForeignKeyConstraint foreignKeyConstraint1 = (ForeignKeyConstraint) constraints.get(1);
        ForeignKeyConstraint foreignKeyConstraint2 = (ForeignKeyConstraint) constraints.get(2);
        ForeignKeyConstraint foreignKeyConstraint3 = (ForeignKeyConstraint) constraints.get(3);
        CheckConstraint check1 = (CheckConstraint) constraints.get(4);
        CheckConstraint check2 = (CheckConstraint) constraints.get(5);
        CheckConstraint check3 = (CheckConstraint) constraints.get(6);
        CheckConstraint check4 = (CheckConstraint) constraints.get(7);
        CheckConstraint check5 = (CheckConstraint) constraints.get(8);
        CheckConstraint check6 = (CheckConstraint) constraints.get(9);
        CheckConstraint check7 = (CheckConstraint) constraints.get(10);

        Assert.assertEquals("FLUG_PS", primConstraint.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("FNR", primConstraint.getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("FLUG_FS_FLC", foreignKeyConstraint1.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("FLC", foreignKeyConstraint1.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("FLC", foreignKeyConstraint1.getReferencedAttribute().getIdentifier());
        Assert.assertEquals("FLUGLINIE", foreignKeyConstraint1.getReferencedTable().getIdentifier());

        Assert.assertEquals("FLUG_FS_VON", foreignKeyConstraint2.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("VON", foreignKeyConstraint2.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("FHC", foreignKeyConstraint2.getReferencedAttribute().getIdentifier());
        Assert.assertEquals("FLUGHAFEN", foreignKeyConstraint2.getReferencedTable().getIdentifier());

        Assert.assertEquals("FLUG_FS_NACH", foreignKeyConstraint3.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("NACH", foreignKeyConstraint3.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("FHC", foreignKeyConstraint3.getReferencedAttribute().getIdentifier());
        Assert.assertEquals("FLUGHAFEN", foreignKeyConstraint3.getReferencedTable().getIdentifier());

        Assert.assertEquals("FLUG_VON_NN", check1.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(UnaryExpression.Operator.ISNOTNULL.name(), ((UnaryExpression) check1.getCheckExpression()).getOperatorName());
        Assert.assertEquals("VON", ((UnaryExpression) check1.getCheckExpression()).getOperand().getIdentifier());

        Assert.assertEquals("FLUG_NACH_NN", check2.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(UnaryExpression.Operator.ISNOTNULL.name(), ((UnaryExpression) check2.getCheckExpression()).getOperatorName());
        Assert.assertEquals("NACH", ((UnaryExpression) check2.getCheckExpression()).getOperand().getIdentifier());

        Assert.assertEquals("FLUG_AB_NN", check3.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(UnaryExpression.Operator.ISNOTNULL.name(), ((UnaryExpression) check3.getCheckExpression()).getOperatorName());
        Assert.assertEquals("AB", ((UnaryExpression) check3.getCheckExpression()).getOperand().getIdentifier());

        Assert.assertEquals("FLUG_AN_NN", check4.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(UnaryExpression.Operator.ISNOTNULL.name(), ((UnaryExpression) check4.getCheckExpression()).getOperatorName());
        Assert.assertEquals("AN", ((UnaryExpression) check4.getCheckExpression()).getOperand().getIdentifier());

        Assert.assertEquals("FLUG_AB_CHK", check5.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("BETWEEN", ((BetweenExpression) check5.getCheckExpression()).getOperatorName());
        Assert.assertEquals("0", ((BetweenExpression) check5.getCheckExpression()).getMin().getIdentifier());
        Assert.assertEquals("2400", ((BetweenExpression) check5.getCheckExpression()).getMax().getIdentifier());

        Assert.assertEquals("FLUG_AN_CHK", check6.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("BETWEEN", ((BetweenExpression) check6.getCheckExpression()).getOperatorName());
        Assert.assertEquals("0", ((BetweenExpression) check6.getCheckExpression()).getMin().getIdentifier());
        Assert.assertEquals("2400", ((BetweenExpression) check6.getCheckExpression()).getMax().getIdentifier());

        Assert.assertEquals("FLUG_VONNACH_CHK", check7.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), ((BinaryExpression) check7.getCheckExpression()).getOperatorName());
        Assert.assertEquals("VON", ((BinaryExpression) check7.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("NACH", ((BinaryExpression) check7.getCheckExpression()).getRightOperand().getIdentifier());

        HorizontalClause horizontalClause = stmt.getHorizontalClause();
        Assert.assertEquals("FLC", horizontalClause.getAttributeIdentifier().getIdentifier());
        Assert.assertEquals("KK", horizontalClause.getFirstBoundary().getLiteral().getIdentifier());
        Assert.assertEquals("MM", horizontalClause.getSecondBoundary().getLiteral().getIdentifier());
        Assert.assertTrue(horizontalClause.getFirstBoundary().getLiteral() instanceof StringLiteral);
        Assert.assertTrue(horizontalClause.getSecondBoundary().getLiteral() instanceof StringLiteral);
    }

    @Test
    public void testVeritcal3AndConstraints() throws ParseException {
        SqlParser parser = new SqlParser("create table FLUGHAFEN ( FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), ZAHL integer, constraint FLUGHAFEN_PS primary key (FHC) , constraint LAND_UN unique (LAND) , constraint FLUGLINIE_ALLIANZ_CHK_LT check (ALLIANZ < 'BlackList') , constraint FLUGLINIE_ALLIANZ_CHK_LE check (ALLIANZ <= 'BlackList') , constraint FLUGLINIE_ALLIANZ_CHK3_GT check (ALLIANZ > 'BlackList') , constraint FLUGLINIE_ALLIANZ_CHK_GE check (ALLIANZ >= 'BlackList') , constraint FLUGLINIE_ALLIANZ_CHK_EQ check (ALLIANZ = 'BlackList') , constraint FLUGLINIE_ALLIANZ_CHK_NE check (ALLIANZ != 'BlackList') , constraint FLUGLINIE_ZAHL_CHK_LT check (ZAHL < 1) , constraint FLUGLINIE_ZAHL_CHK_LE check (ZAHL <= 1) , constraint FLUGLINIE_ZAHL_CHK_GT check (ZAHL > 1) , constraint FLUGLINIE_ZAHL_CHK_GE check (ZAHL >= 1) , constraint FLUGLINIE_ZAHL_CHK_EQ check (ZAHL = 1) , constraint FLUGLINIE_ZAHL_CHK_NE check (ZAHL != 1) )  VERTICAL ((FHC, LAND), (STADT), (NAME))");
        AST ast = parser.parseStatement();
        CreateTableStatement stmt = (CreateTableStatement) ast.getRoot();

        Assert.assertEquals("FLUGHAFEN", stmt.getTableIdentifier().getIdentifier());

        List<TableAttribute> attributes = stmt.getAttributes();
        Assert.assertTrue(attributes.size() == 5);
        TableAttribute fhc = attributes.get(0);
        TableAttribute land = attributes.get(1);
        TableAttribute stadt = attributes.get(2);
        TableAttribute name = attributes.get(3);
        TableAttribute zahl = attributes.get(4);

        Assert.assertEquals("FHC", fhc.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) fhc.getType()).getLength().getIdentifier().equals("3"));
        Assert.assertEquals("LAND", land.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) land.getType()).getLength().getIdentifier().equals("3"));
        Assert.assertEquals("STADT", stadt.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) stadt.getType()).getLength().getIdentifier().equals("50"));
        Assert.assertEquals("NAME", name.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((VarCharType) name.getType()).getLength().getIdentifier().equals("50"));
        Assert.assertEquals("ZAHL", zahl.getIdentifierNode().getIdentifier());
        Assert.assertTrue(((PrimitiveType) zahl.getType()).getPrimitive() == PrimitiveType.Primitive.INTEGER);

        List<Constraint> constraints = stmt.getConstraints();
        Assert.assertTrue(constraints.size() == 14);

        PrimaryKeyConstraint primConstraint = (PrimaryKeyConstraint) constraints.get(0);
        UniqueConstraint uniConstraint = (UniqueConstraint) constraints.get(1);

        CheckConstraint checkStringLT = (CheckConstraint) constraints.get(2);
        CheckConstraint checkStringLE = (CheckConstraint) constraints.get(3);
        CheckConstraint checkStringGT = (CheckConstraint) constraints.get(4);
        CheckConstraint checkStringGE = (CheckConstraint) constraints.get(5);
        CheckConstraint checkStringEQ = (CheckConstraint) constraints.get(6);
        CheckConstraint checkStringNE = (CheckConstraint) constraints.get(7);

        CheckConstraint checkIntegerLT = (CheckConstraint) constraints.get(8);
        CheckConstraint checIntegerLE = (CheckConstraint) constraints.get(9);
        CheckConstraint checkIntegerGT = (CheckConstraint) constraints.get(10);
        CheckConstraint checkIntegerGE = (CheckConstraint) constraints.get(11);
        CheckConstraint checkIntegerEQ = (CheckConstraint) constraints.get(12);
        CheckConstraint checkIntegerNE = (CheckConstraint) constraints.get(13);

        Assert.assertEquals("FLUGHAFEN_PS", primConstraint.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("FHC", primConstraint.getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("LAND_UN", uniConstraint.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals("LAND", uniConstraint.getAttributeIdentifier().getIdentifier());

        Assert.assertEquals("FLUGLINIE_ALLIANZ_CHK_LT", checkStringLT.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LT.name(), ((BinaryExpression) checkStringLT.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ALLIANZ", ((BinaryExpression) checkStringLT.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("BlackList", ((BinaryExpression) checkStringLT.getCheckExpression()).getRightOperand().getIdentifier());
        Assert.assertTrue(((BinaryExpression) checkStringLT.getCheckExpression()).getRightOperand() instanceof StringLiteral);

        Assert.assertEquals("FLUGLINIE_ALLIANZ_CHK_LE", checkStringLE.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), ((BinaryExpression) checkStringLE.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ALLIANZ", ((BinaryExpression) checkStringLE.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("BlackList", ((BinaryExpression) checkStringLE.getCheckExpression()).getRightOperand().getIdentifier());
        Assert.assertTrue(((BinaryExpression) checkStringLE.getCheckExpression()).getRightOperand() instanceof StringLiteral);

        Assert.assertEquals("FLUGLINIE_ALLIANZ_CHK3_GT", checkStringGT.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GT.name(), ((BinaryExpression) checkStringGT.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ALLIANZ", ((BinaryExpression) checkStringGT.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("BlackList", ((BinaryExpression) checkStringGT.getCheckExpression()).getRightOperand().getIdentifier());
        Assert.assertTrue(((BinaryExpression) checkStringGT.getCheckExpression()).getRightOperand() instanceof StringLiteral);

        Assert.assertEquals("FLUGLINIE_ALLIANZ_CHK_GE", checkStringGE.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GEQ.name(), ((BinaryExpression) checkStringGE.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ALLIANZ", ((BinaryExpression) checkStringGE.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("BlackList", ((BinaryExpression) checkStringGE.getCheckExpression()).getRightOperand().getIdentifier());
        Assert.assertTrue(((BinaryExpression) checkStringGE.getCheckExpression()).getRightOperand() instanceof StringLiteral);

        Assert.assertEquals("FLUGLINIE_ALLIANZ_CHK_EQ", checkStringEQ.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.EQ.name(), ((BinaryExpression) checkStringEQ.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ALLIANZ", ((BinaryExpression) checkStringEQ.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("BlackList", ((BinaryExpression) checkStringEQ.getCheckExpression()).getRightOperand().getIdentifier());
        Assert.assertTrue(((BinaryExpression) checkStringEQ.getCheckExpression()).getRightOperand() instanceof StringLiteral);

        Assert.assertEquals("FLUGLINIE_ALLIANZ_CHK_NE", checkStringNE.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), ((BinaryExpression) checkStringNE.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ALLIANZ", ((BinaryExpression) checkStringNE.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("BlackList", ((BinaryExpression) checkStringNE.getCheckExpression()).getRightOperand().getIdentifier());
        Assert.assertTrue(((BinaryExpression) checkStringNE.getCheckExpression()).getRightOperand() instanceof StringLiteral);

        Assert.assertEquals("FLUGLINIE_ZAHL_CHK_LT", checkIntegerLT.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LT.name(), ((BinaryExpression) checkIntegerLT.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ZAHL", ((BinaryExpression) checkIntegerLT.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("1", ((BinaryExpression) checkIntegerLT.getCheckExpression()).getRightOperand().getIdentifier());

        Assert.assertEquals("FLUGLINIE_ZAHL_CHK_LE", checIntegerLE.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.LEQ.name(), ((BinaryExpression) checIntegerLE.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ZAHL", ((BinaryExpression) checIntegerLE.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("1", ((BinaryExpression) checIntegerLE.getCheckExpression()).getRightOperand().getIdentifier());

        Assert.assertEquals("FLUGLINIE_ZAHL_CHK_GT", checkIntegerGT.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GT.name(), ((BinaryExpression) checkIntegerGT.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ZAHL", ((BinaryExpression) checkIntegerGT.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("1", ((BinaryExpression) checkIntegerGT.getCheckExpression()).getRightOperand().getIdentifier());

        Assert.assertEquals("FLUGLINIE_ZAHL_CHK_GE", checkIntegerGE.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.GEQ.name(), ((BinaryExpression) checkIntegerGE.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ZAHL", ((BinaryExpression) checkIntegerGE.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("1", ((BinaryExpression) checkIntegerGE.getCheckExpression()).getRightOperand().getIdentifier());

        Assert.assertEquals("FLUGLINIE_ZAHL_CHK_EQ", checkIntegerEQ.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.EQ.name(), ((BinaryExpression) checkIntegerEQ.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ZAHL", ((BinaryExpression) checkIntegerEQ.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("1", ((BinaryExpression) checkIntegerEQ.getCheckExpression()).getRightOperand().getIdentifier());

        Assert.assertEquals("FLUGLINIE_ZAHL_CHK_NE", checkIntegerNE.getConstraintIdentifier().getIdentifier());
        Assert.assertEquals(BinaryExpression.Operator.NEQ.name(), ((BinaryExpression) checkIntegerNE.getCheckExpression()).getOperatorName());
        Assert.assertEquals("ZAHL", ((BinaryExpression) checkIntegerNE.getCheckExpression()).getLeftOperand().getIdentifier());
        Assert.assertEquals("1", ((BinaryExpression) checkIntegerNE.getCheckExpression()).getRightOperand().getIdentifier());
    }
}
