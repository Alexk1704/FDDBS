package fdbs.sql.parser.ast.constraint;

import fdbs.sql.parser.ast.expression.*;
import fdbs.sql.parser.ast.identifier.*;

/**
 * A create table CHECK constraint.
 */
public class CheckConstraint extends Constraint {

    private Expression expression;

    /**
     * The Constructor for a create table CHECK constraint node.
     *
     * @param constraintIdentifier A constraint identifier node which represent
     * the name of the create table CHECK constraint.
     * @param expression A expression node which represent a check unary or
     * binary expression.
     */
    public CheckConstraint(ConstraintIdentifier constraintIdentifier, Expression expression) {
        super(constraintIdentifier);

        this.expression = expression;
        this.addChild(this.expression);
    }

    /**
     * Gets the expression node which represent a check unary or binary
     * expression.
     *
     * @return Returns the expression node which represent a check unary or
     * binary expression.
     */
    public Expression getCheckExpression() {
        return expression;
    }
}
