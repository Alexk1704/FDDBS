package fdbs.sql.parser.ast.constraint;

import fdbs.sql.parser.ast.identifier.*;

/**
 * A create table FOREIGN KEY constraint.
 */
public class ForeignKeyConstraint extends Constraint {

    private AttributeIdentifier attributeIdentifier;
    private TableIdentifier referencedTable;

    private AttributeIdentifier referencedAttribute;

    /**
     * The Constructor for a create table FOREIGN KEY constraint node.
     *
     * @param constraintIdentifier A constraint identifier node which represent
     * the name of the create table FOREIGN KEY constraint.
     * @param attributeIdentifier An attribute identifier node which represent
     * the foreign key attribute name.
     * @param referencedTable A table identifier node which represent the
     * foreign key referenced table name.
     * @param referencedAttribute An attribute identifier node which represent
     * the foreign key referenced attribute name.
     */
    public ForeignKeyConstraint(ConstraintIdentifier constraintIdentifier, AttributeIdentifier attributeIdentifier, TableIdentifier referencedTable, AttributeIdentifier referencedAttribute) {
        super(constraintIdentifier);

        this.attributeIdentifier = attributeIdentifier;
        this.addChild(this.attributeIdentifier);

        this.referencedTable = referencedTable;
        this.addChild(this.referencedTable);

        this.referencedAttribute = referencedAttribute;
        this.addChild(this.referencedAttribute);
    }

    /**
     * Gets the attribute identifier node which represent the foreign key
     * attribute name.
     *
     * @return Returns the attribute identifier node which represent the foreign
     * key attribute name.
     */
    public AttributeIdentifier getAttributeIdentifier() {
        return attributeIdentifier;
    }

    /**
     * Gets the table identifier node which represent the foreign key referenced
     * table name.
     *
     * @return Returns the table identifier node which represent the foreign key
     * referenced table name.
     */
    public TableIdentifier getReferencedTable() {
        return referencedTable;
    }

    /**
     * Gets the attribute identifier node which represent the foreign key
     * referenced attribute name.
     *
     * @return Returns the attribute identifier node which represent the foreign
     * key referenced attribute name.
     */
    public AttributeIdentifier getReferencedAttribute() {
        return referencedAttribute;
    }
}
