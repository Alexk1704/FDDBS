package fdbs.sql.parser.ast.literal;

import fdbs.sql.parser.*;

/**
 * A integer literal.
 */
public class IntegerLiteral extends Literal {

    /**
     * The Constructor for a integer literal node.
     *
     * @param token A parser token which represent the parsed integer literal.
     */
    public IntegerLiteral(Token token) {
        super(token);
    }

    /**
     * The Constructor for a integer literal node.
     *
     * @param value A integer literal.
     */
    public IntegerLiteral(String value) {
        super(value);
    }
}
