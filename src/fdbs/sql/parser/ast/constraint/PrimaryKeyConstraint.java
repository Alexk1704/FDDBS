package fdbs.sql.parser.ast.constraint;

import fdbs.sql.parser.ast.identifier.*;

/**
 * A create table PRIMARY KEY constraint.
 */
public class PrimaryKeyConstraint extends Constraint {

    private AttributeIdentifier attributeIdentifier;

    /**
     * The Constructor for a create table PRIMARY KEY constraint node.
     *
     * @param constraintIdentifier A constraint identifier node which represent
     * the name of the create table PRIMARY KEY constraint.
     * @param attributeIdentifier An attribute identifier node which represent
     * the primary key attribute name.
     */
    public PrimaryKeyConstraint(ConstraintIdentifier constraintIdentifier, AttributeIdentifier attributeIdentifier) {
        super(constraintIdentifier);

        this.attributeIdentifier = attributeIdentifier;
        this.addChild(this.attributeIdentifier);
    }

    /**
     * Gets the attribute identifier node which represent the primary key
     * attribute name.
     *
     * @return Returns the attribute identifier node which represent the primary
     * key attribute name.
     */
    public AttributeIdentifier getAttributeIdentifier() {
        return attributeIdentifier;
    }
}
