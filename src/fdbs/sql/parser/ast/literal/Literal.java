package fdbs.sql.parser.ast.literal;

import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.*;

/**
 * A literal.
 */
public abstract class Literal extends ASTNode {

    protected Token token;

    /**
     * The Constructor for a literal node.
     *
     * @param token A parser token which represent the parsed literal.
     */
    public Literal(Token token) {
        super();

        this.token = token;
        super.setIdentifier(token.image);
    }

    /**
     * The Constructor for a literal node.
     *
     * @param value A parsed literal.
     */
    public Literal(String value) {
        super();

        this.token = null;
        super.setIdentifier(value);
    }
}
