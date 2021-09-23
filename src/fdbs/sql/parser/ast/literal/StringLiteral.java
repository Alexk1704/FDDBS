package fdbs.sql.parser.ast.literal;

import fdbs.sql.parser.*;

/**
 * A string literal.
 */
public class StringLiteral extends Literal {

    /**
     * The Constructor for a string literal node.
     *
     * @param token A parser token which represent the parsed string literal.
     */
    public StringLiteral(Token token) {
        super(token);
    }

    /**
     * The Constructor for a string literal node.
     *
     * @param value A string literal.
     */
    public StringLiteral(String value) {
        super(value);
    }

    @Override
    public String getIdentifier() {
        String identifier;

        if (token != null) {
            identifier = token.image.substring(1, token.image.length() - 1);
        } else {
            identifier = this.identifier;
        }

        return identifier;
    }
}
