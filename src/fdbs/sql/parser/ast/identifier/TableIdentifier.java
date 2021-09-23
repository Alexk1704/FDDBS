package fdbs.sql.parser.ast.identifier;

import fdbs.sql.parser.*;

/**
 * A table identifier.
 */
public class TableIdentifier extends Identifier {

    /**
     * The Constructor for a table identifier node.
     *
     * @param idToken A parser token which represent the parsed table
     * identifier.
     */
    public TableIdentifier(Token idToken) {
        super(idToken);
    }
}
