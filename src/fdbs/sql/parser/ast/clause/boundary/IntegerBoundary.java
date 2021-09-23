package fdbs.sql.parser.ast.clause.boundary;

import fdbs.sql.parser.ast.literal.*;

/**
 * A HORIZONTAL clause integer boundary.
 */
public class IntegerBoundary extends Boundary {

    /**
     * The Constructor for a HORIZONTAL clause integer boundary node.
     *
     * @param literal A literal node which represent a integer boundary for a
     * HORIZONTAL clause.
     */
    public IntegerBoundary(Literal literal) {
        super(literal);
    }
}
