package fdbs.sql.parser.ast.clause.boundary;

import fdbs.sql.parser.ast.literal.*;

/**
 * A HORIZONTAL clause string boundary.
 */
public class StringBoundary extends Boundary {

    /**
     * The Constructor for a HORIZONTAL clause integer boundary node.
     *
     * @param literal A literal node which represent a string boundary for a
     * HORIZONTAL clause.
     */
    public StringBoundary(Literal literal) {
        super(literal);
    }
}
