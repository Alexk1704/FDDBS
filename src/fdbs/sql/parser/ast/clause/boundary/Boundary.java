package fdbs.sql.parser.ast.clause.boundary;

import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.ast.literal.*;

/**
 * A HORIZONTAL clause boundary.
 */
public abstract class Boundary extends ASTNode {

    private Literal literal;

    /**
     * The Constructor for a HORIZONTAL clause boundary node.
     *
     * @param literal A literal node which represent a boundary for a HORIZONTAL
     * clause.
     */
    public Boundary(Literal literal) {
        super();

        this.literal = literal;
        this.addChild(this.literal);
    }

    /**
     * Gets the literal node which represent a boundary for a HORIZONTAL clause.
     *
     * @return Returns the literal node which represent a boundary for a
     * HORIZONTAL clause.
     */
    public Literal getLiteral() {
        return literal;
    }
}
