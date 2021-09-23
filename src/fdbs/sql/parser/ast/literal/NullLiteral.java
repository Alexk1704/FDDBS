package fdbs.sql.parser.ast.literal;

import fdbs.sql.parser.*;

/**
 * A null literal.
 */
public class NullLiteral extends Literal {

    /**
     * The Constructor for a null literal node.
     *
     * @param token A parser token which represent the parsed null literal.
     */
    public NullLiteral(Token token) {
        super(token);
    }
    
    /**
     * The Constructor for a null literal node.
     *
     * @param value A null literal.
     */
    public NullLiteral(String value) {
        super(value);
    }
}
