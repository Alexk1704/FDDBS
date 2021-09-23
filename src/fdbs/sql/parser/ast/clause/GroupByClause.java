package fdbs.sql.parser.ast.clause;

import fdbs.sql.parser.ast.identifier.*;

/**
 * A sql statement GROUP BY clause.
 */
public class GroupByClause extends Clause {

    FullyQualifiedAttributeIdentifier fQAttributeIdentifier;

    /**
     * The Constructor for a GROUP BY clause node.
     *
     * @param fQAttributeIdentifier A fully qualified attribute identifier node
     * which represent a full qualified attribute name.
     */
    public GroupByClause(FullyQualifiedAttributeIdentifier fQAttributeIdentifier) {
        this.fQAttributeIdentifier = fQAttributeIdentifier;
        this.addChild(fQAttributeIdentifier);
    }

    /**
     * Gets the full qualified attribute identifier node which represent a full
     * qualified attribute name.
     *
     * @return Returns the full qualified attribute identifier node which
     * represent a full qualified attribute name.
     */
    public FullyQualifiedAttributeIdentifier getFQAttributeIdentifier() {
        return fQAttributeIdentifier;
    }
}
