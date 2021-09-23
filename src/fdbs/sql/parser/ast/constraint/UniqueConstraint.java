package fdbs.sql.parser.ast.constraint;

import fdbs.sql.parser.ast.identifier.*;

/**
 * A create table UNIQUE constraint.
 */
public class UniqueConstraint extends Constraint {

    private AttributeIdentifier attributeIdentifier;

    /**
     * The Constructor for a create table UNIQUE constraint node.
     *
     * @param constraintIdentifier A constraint identifier node which represent
     * the name of the create table UNIQUE constraint.
     * @param attributeIdentifier An attribute identifier node which represent
     * the unique attribute name.
     */
    public UniqueConstraint(ConstraintIdentifier constraintIdentifier, AttributeIdentifier attributeIdentifier) {
        super(constraintIdentifier);

        this.attributeIdentifier = attributeIdentifier;
        this.addChild(this.attributeIdentifier);
    }

    /**
     * Gets the attribute identifier node which represent the unique attribute
     * name.
     *
     * @return Returns the attribute identifier node which represent the unique
     * attribute name.
     */
    public AttributeIdentifier getAttributeIdentifier() {
        return attributeIdentifier;
    }
}
