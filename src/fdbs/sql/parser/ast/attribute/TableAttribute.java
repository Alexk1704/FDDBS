package fdbs.sql.parser.ast.attribute;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.type.*;
import fdbs.sql.parser.ast.literal.*;

/**
 * A table attribute.
 */
public class TableAttribute extends Attribute {

    /**
     * The Constructor for a table attribute node.
     *
     * @param identifier A identifier node which represent name of this table
     * attribute.
     * @param type A type node which represent the type of this table attribute.
     * @param defaultValue A literal node which represent the default value of
     * this table attribute.
     */
    public TableAttribute(Identifier identifier, Type type, Literal defaultValue) {
        super(identifier, type, defaultValue);
    }
}
