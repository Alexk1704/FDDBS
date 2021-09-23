package fdbs.sql.parser.ast.type;

import fdbs.sql.parser.ast.ASTNode;

/**
 * A type.
 */
public abstract class Type extends ASTNode {

    /**
     * The Constructor for a type node.
     */
    public Type() {
        super();
    }

    /**
     * Gets type descriptor.
     *
     * @return Returns the type descriptor.
     */
    public abstract String getTypeDescriptor();
}
