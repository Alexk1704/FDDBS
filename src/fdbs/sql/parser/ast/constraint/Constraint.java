package fdbs.sql.parser.ast.constraint;

import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.ast.identifier.ConstraintIdentifier;

/**
 * A create table constraint.
 */
public abstract class Constraint extends ASTNode {

    private ConstraintIdentifier constraintIdentifier;

    /**
     * The Constructor for a create table constraint node.
     *
     * @param constraintIdentifier A constraint identifier node which represent
     * the name of the create table constraint.
     */
    public Constraint(ConstraintIdentifier constraintIdentifier) {
        super();

        this.constraintIdentifier = constraintIdentifier;
        this.addChild(this.constraintIdentifier);
    }

    /**
     * Gets the constraint identifier node which represent the name of the
     * create table constraint.
     *
     * @return Returns the constraint identifier node which represent the name
     * of the create table constraint.
     */
    public ConstraintIdentifier getConstraintIdentifier() {
        return constraintIdentifier;
    }
}
