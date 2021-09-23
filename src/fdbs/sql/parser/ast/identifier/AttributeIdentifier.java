package fdbs.sql.parser.ast.identifier;

import fdbs.sql.parser.*;

/**
 * An attribute identifier.
 */
public class AttributeIdentifier extends Identifier {

    /**
     * The Constructor for an attribute identifier node.
     *
     * @param idToken A parser token which represent the parsed attribute
     * identifier.
     */
    public AttributeIdentifier(Token idToken) {
        super(idToken);
    }

    /**
     * The Constructor for an attribute identifier node.
     *
     * @param identifier A attribute identifier.
     */
    public AttributeIdentifier(String identifier) {
        super(identifier);
    }
}
