package fdbs.sql.parser.ast.expression;

import fdbs.sql.parser.ast.identifier.*;

/**
 * An unary expression.
 */
public class UnaryExpression extends Expression {

    private Operator operator;
    private AttributeIdentifier operand;

    /**
     * The unary expression operator enumeration .
     */
    public static enum Operator {
        ISNOTNULL
    }

    /**
     * The Constructor for an unary expression node.
     *
     * @param operand An attribute identifier node which represent the operand
     * of the unary expression.
     * @param operator The operator of this unary expression.
     */
    public UnaryExpression(AttributeIdentifier operand, Operator operator) {
        super();

        this.operand = operand;
        this.addChild(this.operand);

        this.operator = operator;
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
     * Gets the attribute identifier node which represent the operand of the
     * unary expression.
     *
     * @return Returns the attribute identifier node which represent the operand
     * of the unary expression.
     */
    public AttributeIdentifier getOperand() {
        return operand;
    }
}
