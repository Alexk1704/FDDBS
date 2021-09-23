package fdbs.sql.parser.ast.type;

import fdbs.sql.parser.ast.literal.*;

/**
 * A var char type.
 */
public class VarCharType extends Type {

    private IntegerLiteral length;

    /**
     * The Constructor for a var char type node.
     *
     * @param length A integer literal node which represent the length of this
     * var char type.
     */
    public VarCharType(IntegerLiteral length) {
        super();

        this.length = length;
        this.addChild(this.length);
    }

    @Override
    public String getIdentifier() {
        return "VARCHAR";
    }

    @Override
    public String getTypeDescriptor() {
        return String.format("%s(%s)", getIdentifier(), getLength().getIdentifier());
    }

    /**
     * Gets the integer literal node which represent the length of this var char
     * type.
     *
     * @return Returns the integer literal node which represent the length of
     * this var char type.
     */
    public IntegerLiteral getLength() {
        return this.length;
    }
}
