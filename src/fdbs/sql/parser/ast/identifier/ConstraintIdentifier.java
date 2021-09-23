package fdbs.sql.parser.ast.identifier;

import fdbs.sql.parser.*;

/**
 * A constraint identifier.
 */
public class ConstraintIdentifier extends Identifier {

    /**
     * The Constructor for a constraint identifier node.
     *
     * @param idToken A parser token which represent the parsed constraint
     * identifier.
     */
    public ConstraintIdentifier(Token idToken) {
        super(idToken);
    }
}
