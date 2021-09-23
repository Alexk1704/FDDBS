package fdbs.sql.parser.ast.identifier;

import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.*;

/**
 * An identifier.
 */
public abstract class Identifier extends ASTNode {

    private final Token idToken;

    /**
     * The Constructor for an identifier node.
     *
     * @param idToken A parser token which represent the parsed identifier.
     */
    public Identifier(Token idToken) {
        super();

        this.idToken = idToken;
        this.setIdentifier(this.idToken.image);
    }

    /**
     * The Constructor for an identifier node.
     *
     * @param identifier A identifier.
     */
    public Identifier(String identifier) {
        super();

        this.idToken = null;
        this.setIdentifier(identifier);
    }

    /**
     * Gets the parser token which represent the parsed identifier.
     *
     * @return Returns the parser token which represent the parsed identifier.
     */
    public Token getToken() {
        return this.idToken;
    }
}
