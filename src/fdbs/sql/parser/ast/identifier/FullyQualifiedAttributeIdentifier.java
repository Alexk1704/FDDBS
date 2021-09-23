package fdbs.sql.parser.ast.identifier;

import fdbs.sql.parser.*;

/**
 * A fully qualified attribute identifier.
 */
public class FullyQualifiedAttributeIdentifier extends Identifier {

    private AttributeIdentifier attributeIdentifier;

    /**
     * The Constructor for a fully qualified attribute identifier node.
     *
     * @param tableIdentifier A parser token which represent the parsed table
     * name of this fully qualified attribute identifier.
     * @param attributeIdentifier An attribute identifier node which represent
     * the attribute name of this fully qualified attribute identifier.
     */
    public FullyQualifiedAttributeIdentifier(Token tableIdentifier, AttributeIdentifier attributeIdentifier) {
        super(tableIdentifier);

        this.attributeIdentifier = attributeIdentifier;
        this.addChild(this.attributeIdentifier);
    }

    /**
     * The Constructor for a fully qualified attribute identifier node.
     *
     * @param tableIdentifier A table name of this fully qualified attribute
     * identifier.
     * @param attributeIdentifier An attribute name of this fully qualified
     * attribute identifier.
     */
    public FullyQualifiedAttributeIdentifier(String tableIdentifier, String attributeIdentifier) {
        super(tableIdentifier);

        this.attributeIdentifier = new AttributeIdentifier(attributeIdentifier);
        this.addChild(this.attributeIdentifier);
    }

    /**
     * Gets the attribute identifier node which represent the attribute name of
     * this fully qualified attribute identifier.
     *
     * @return Returns the the attribute identifier node which represent the
     * attribute name of this fully qualified attribute identifier.
     */
    public AttributeIdentifier getAttributeIdentifier() {
        return attributeIdentifier;
    }

    public String getFullyQualifiedIdentifier() {
        return String.format("%s.%s", this.getIdentifier(), this.attributeIdentifier.getIdentifier());
    }
}
