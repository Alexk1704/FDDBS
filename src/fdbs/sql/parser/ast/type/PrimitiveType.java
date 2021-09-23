package fdbs.sql.parser.ast.type;

import fdbs.sql.parser.*;

/**
 * A primitive type.
 */
public class PrimitiveType extends Type {

    /**
     * The primitive type enumeration .
     */
    public static enum Primitive {
        INTEGER
    }

    private Primitive type;

    /**
     * The Constructor for a primitive type node.
     *
     * @param typeName The tpye name of this primitive type.
     * @throws ParseException
     */
    public PrimitiveType(String typeName) throws ParseException {
        super();

        this.type = this.stringToType(typeName.toUpperCase());
    }

    /**
     * The Constructor for a primitive type node.
     *
     * @param type The tpye of this primitive type.
     */
    public PrimitiveType(Primitive type) {
        super();

        this.type = type;
    }

    private Primitive stringToType(String name) throws ParseException {
        for (Primitive primType : Primitive.values()) {
            if (primType.name().equals(name)) {
                return primType;
            }
        }
        throw new ParseException(String.format("%s: Unknown primitive type %s", this.getClass().getSimpleName(), name));
    }

    /**
     * Gets the primitive type.
     *
     * @return Returns the primitive type.
     */
    public Primitive getPrimitive() {
        return type;
    }

    @Override
    public String getIdentifier() {
        return this.getPrimitive().name();
    }

    @Override
    public String getTypeDescriptor() {
        return getPrimitive().name();
    }
}
