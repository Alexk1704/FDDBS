package fdbs.sql.parser.ast.identifier;

import fdbs.sql.parser.*;

/**
 * An all attribute identifier.
 */
public class AllAttributeIdentifier extends Identifier {

    /**
     * The Constructor for an all attribute identifier node.
     *
     * @param idToken A parser token which represent the parsed all attribute
     * identifier.
     */
    public AllAttributeIdentifier(Token idToken) {
        super(idToken);
    }
}
