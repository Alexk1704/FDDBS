package fdbs.sql.parser.ast.expression;

import fdbs.sql.FedException;
import fdbs.sql.parser.ast.*;
import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.literal.*;
import fdbs.sql.parser.ast.function.*;

/**
 * An binary expression.
 */
public class BinaryExpression extends Expression {

    private FullyQualifiedAttributeIdentifier leftFQAttributeIdentifier;
    private BinaryExpression leftBinaryExpression;
    private Function leftFunction;
    private AttributeIdentifier leftOperand;

    private Operator operator;

    private FullyQualifiedAttributeIdentifier rightFQAttributeIdentifier;
    private AttributeIdentifier rightIdentifierOperand;
    private Literal rightLiteralOperand;
    private BinaryExpression rightBinaryExpression;

    /**
     * The binary expression operator enumeration .
     */
    public static enum Operator {
        EQ, NEQ, LT, GT, LEQ, GEQ, AND, OR
    }

    /**
     * The Constructor for a binary expression node.
     *
     * @param leftOperand An attribute identifier node which represent the left
     * operand of this binary expression.
     * @param operator The operator of this unary expression.
     * @param rightIdentifierOperand An attribute identifier node which
     * represent the right operand of this binary expression.
     */
    public BinaryExpression(AttributeIdentifier leftOperand, Operator operator, AttributeIdentifier rightIdentifierOperand) {
        super();

        this.leftOperand = leftOperand;
        this.addChild(this.leftOperand);

        this.operator = operator;

        this.rightIdentifierOperand = rightIdentifierOperand;
        this.addChild(this.rightIdentifierOperand);
    }

    /**
     * The Constructor for a binary expression node.
     *
     * @param leftOperand An attribute identifier node which represent the left
     * operand of this binary expression.
     * @param operator The operator of this unary expression.
     * @param rightLiteralOperand An attribute identifier node which represent
     * the right operand of this binary expression.
     */
    public BinaryExpression(AttributeIdentifier leftOperand, Operator operator, Literal rightLiteralOperand) {
        super();

        this.leftOperand = leftOperand;
        this.addChild(this.leftOperand);

        this.operator = operator;

        this.rightLiteralOperand = rightLiteralOperand;
        this.addChild(this.rightLiteralOperand);
    }

    /**
     * The Constructor for a binary expression node.
     *
     * @param leftBinaryExpression A binary expression node which represent the
     * left operand of this binary expression.
     * @param operator The operator of this unary expression.
     * @param rightBinaryExpression A binary expression node which represent the
     * right operand of this binary expression.
     */
    public BinaryExpression(BinaryExpression leftBinaryExpression, Operator operator, BinaryExpression rightBinaryExpression) {
        super();

        this.leftBinaryExpression = leftBinaryExpression;
        this.addChild(this.leftBinaryExpression);

        this.operator = operator;

        this.rightBinaryExpression = rightBinaryExpression;
        this.addChild(this.rightBinaryExpression);
    }

    /**
     * The Constructor for a binary expression node.
     *
     * @param leftFQAttributeIdentifier A fully qualified attribute identifier
     * node which represent the left operand of this binary expression.
     * @param operator The operator of this unary expression.
     * @param rightLiteralOperand A fully qualified attribute identifier node
     * which represent the right operand of this binary expression.
     */
    public BinaryExpression(FullyQualifiedAttributeIdentifier leftFQAttributeIdentifier, Operator operator, Literal rightLiteralOperand) {
        super();

        this.leftFQAttributeIdentifier = leftFQAttributeIdentifier;
        this.addChild(this.leftFQAttributeIdentifier);

        this.operator = operator;

        this.rightLiteralOperand = rightLiteralOperand;
        this.addChild(this.rightLiteralOperand);
    }

    /**
     * The Constructor for a binary expression node.
     *
     * @param leftFQAttributeIdentifier A fully qualified attribute identifier
     * node which represent the left operand of this binary expression.
     * @param operator The operator of this unary expression.
     * @param rightFQAttributeIdentifier A fully qualified attribute identifier
     * node which represent the right operand of this binary expression.
     */
    public BinaryExpression(FullyQualifiedAttributeIdentifier leftFQAttributeIdentifier, Operator operator, FullyQualifiedAttributeIdentifier rightFQAttributeIdentifier) {
        super();

        this.leftFQAttributeIdentifier = leftFQAttributeIdentifier;
        this.addChild(this.leftFQAttributeIdentifier);

        this.operator = operator;

        this.rightFQAttributeIdentifier = rightFQAttributeIdentifier;
        this.addChild(this.rightFQAttributeIdentifier);
    }

    /**
     * The Constructor for a binary expression node.
     *
     * @param leftFunction A function node which represent the left operand of
     * this binary expression.
     * @param operator The operator of this unary expression.
     * @param rightLiteralOperand A literal node which represent the right
     * operand of this binary expression.
     */
    public BinaryExpression(Function leftFunction, Operator operator, Literal rightLiteralOperand) {
        super();

        this.leftFunction = leftFunction;
        this.addChild(this.leftFunction);

        this.operator = operator;

        this.rightLiteralOperand = rightLiteralOperand;
        this.addChild(this.rightLiteralOperand);
    }

    /**
     * Gets the abstract syntax node which represent the left operand of this
     * binary expression.
     *
     * @return Returns the abstract syntax node which represent the left operand
     * of this binary expression.
     */
    public ASTNode getLeftOperand() {
        return leftFQAttributeIdentifier != null
                ? leftFQAttributeIdentifier : leftBinaryExpression != null
                        ? leftBinaryExpression : leftFunction != null
                                ? leftFunction : leftOperand;
    }

    /**
     * Gets the operator name.
     *
     * @return Returns the the operator name.
     */
    @Override
    public String getOperatorName() {
        return operator.name();
    }

    /**
     * Gets the operator.
     *
     * @return Returns the the operator name.
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * Gets the abstract syntax node which represent the right operand of this
     * binary expression.
     *
     * @return Returns the abstract syntax node which represent the right
     * operand of this binary expression.
     */
    public ASTNode getRightOperand() {
        return rightFQAttributeIdentifier != null
                ? rightFQAttributeIdentifier : rightIdentifierOperand != null
                        ? rightIdentifierOperand : rightLiteralOperand != null
                                ? rightLiteralOperand : rightBinaryExpression;
    }

    /**
     * Is the current binary expression a join condition?
     *
     * @return Returns true when the current binary expression is a join
     * condition.
     */
    public boolean isJoinCondition() {
        return leftFQAttributeIdentifier != null && rightFQAttributeIdentifier != null;
    }

    /**
     * Is the current binary expression a non join condition?
     *
     * @return Returns true when the current binary expression is a non join
     * condition.
     */
    public boolean isNoNJoinCondition() {
        return leftFunction == null && leftBinaryExpression == null && rightFQAttributeIdentifier == null && rightBinaryExpression == null;
    }

    /**
     * Is the current binary expression a nested binary expression with other
     * binary expressions as children.
     *
     * @return Returns true when current binary expression have other binary
     * expressions as children.
     */
    public boolean isANestedBinaryExpression() {
        return (operator == Operator.AND || operator == Operator.OR) && leftBinaryExpression != null && rightBinaryExpression != null;
    }

    public String getSqlOperatorName() throws FedException {
        String compareOperator = "";
        switch (this.operator) {
            case LEQ:
                compareOperator = "<=";
                break;
            case GEQ:
                compareOperator = ">=";
                break;
            case EQ:
                compareOperator = "=";
                break;
            case NEQ:
                compareOperator = "!=";
                break;
            case LT:
                compareOperator = "<";
                break;
            case GT:
                compareOperator = ">";
                break;
            default:
                throw new FedException(String.format("%s: Unsupported operand", this.getClass().getSimpleName()));
        }

        return compareOperator;
    }
}
