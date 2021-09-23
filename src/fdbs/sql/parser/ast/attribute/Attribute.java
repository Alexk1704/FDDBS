package fdbs.sql.parser.ast.attribute;

import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.type.*;
import fdbs.sql.parser.ast.literal.*;

/**
 * An attribute.
 */
public abstract class Attribute extends ASTNode {

    private Identifier identifierNode;
    private Type type;
    private Literal defaultValue;

    /**
     * The Constructor for an attribute node.
     *
     * @param identifierNode A identifier node which represent name of this
     * table attribute.
     * @param type A type node which represent the type of this table attribute.
     * @param defaultValue A literal node which represent the default value of
     * this table attribute.
     */
    public Attribute(Identifier identifierNode, Type type, Literal defaultValue) {
        super();

        this.identifierNode = identifierNode;
        this.addChild(this.identifierNode);

        this.type = type;
        this.addChild(this.type);

        if (defaultValue != null) {
            this.defaultValue = defaultValue;
            this.addChild(this.defaultValue);
        }
    }

    /**
     * Gets the identifier node which represent name of the attribute.
     *
     * @return Returns the identifier node which represent name of the
     * attribute.
     */
    public Identifier getIdentifierNode() {
        return identifierNode;
    }

    /**
     * Gets the type node which represent the type of the attribute.
     *
     * @return Returns the type node which represent the type of the attribute.
     */
    public Type getType() {
        return type;
    }

    /**
     * Gets the literal node which represent the default value of the attribute.
     *
     * @return Returns the literal node which represent the default value of the
     * attribute.
     */
    public Literal getDefaultValue() {
        return defaultValue;
    }
}
