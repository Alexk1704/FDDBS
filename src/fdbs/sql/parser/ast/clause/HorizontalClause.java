package fdbs.sql.parser.ast.clause;

import fdbs.sql.parser.ast.clause.boundary.*;
import fdbs.sql.parser.ast.identifier.*;

/**
 * A sql statement HORIZONTAL clause.
 */
public class HorizontalClause extends Clause {

    private AttributeIdentifier attributeIdentifier;
    private Boundary firstBoundary;
    private Boundary secondBoundary;

    /**
     * The Constructor for a HORIZONTAL clause node.
     *
     * @param attributeIdentifier An attribute identifier node which represent
     * an attribute name.
     * @param firstBoundary A boundary node which represent the first boundy of
     * the HORIZONTAL clause.
     * @param secondBoundary A boundary node which represent the second boundy
     * of the HORIZONTAL clause.
     */
    public HorizontalClause(AttributeIdentifier attributeIdentifier, Boundary firstBoundary, Boundary secondBoundary) {
        super();

        this.attributeIdentifier = attributeIdentifier;
        this.addChild(this.attributeIdentifier);

        if (firstBoundary != null) {
            this.firstBoundary = firstBoundary;
            this.addChild(this.firstBoundary);
        }

        this.secondBoundary = secondBoundary;
        this.addChild(this.secondBoundary);
    }

    /**
     * Gets the attribute identifier node which represent an attribute name.
     *
     * @return Returns the attribute identifier node which represent an
     * attribute name.
     */
    public AttributeIdentifier getAttributeIdentifier() {
        return attributeIdentifier;
    }

    /**
     * Gets the first boundary node which represent the first boundary of the
     * HORIZONTAL clause.
     *
     * @return Returns the first boundary node which represent the first
     * boundary of the HORIZONTAL clause.
     */
    public Boundary getFirstBoundary() {
        return firstBoundary;
    }

    /**
     * Gets the second boundary node which represent the second boundy of the
     * HORIZONTAL clause.
     *
     * @return Returns the second boundary node which represent the second
     * boundy of the HORIZONTAL clause.
     */
    public Boundary getSecondBoundary() {
        return secondBoundary;
    }
}
