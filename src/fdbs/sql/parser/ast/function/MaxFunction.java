package fdbs.sql.parser.ast.function;

import fdbs.sql.parser.ast.identifier.*;

/**
 * A max function.
 */
public class MaxFunction extends Function {

    private FullyQualifiedAttributeIdentifier fQAttributeIdentifier;

    /**
     * The Constructor for a max function node.
     *
     * @param fQAttributeIdentifier A fully qualified attribute identifier node
     * which represent the attribute name of the max function.
     */
    public MaxFunction(FullyQualifiedAttributeIdentifier fQAttributeIdentifier) {
        super();

        this.fQAttributeIdentifier = fQAttributeIdentifier;
        this.addChild(this.fQAttributeIdentifier);
    }

    /**
     * Gets the fully qualified attribute identifier node which represent the
     * attribute name of the max function.
     *
     * @return Returns the fully qualified attribute identifier node which
     * represent the attribute name of the max function.
     */
    public FullyQualifiedAttributeIdentifier getFQAttributeIdentifier() {
        return fQAttributeIdentifier;
    }

    @Override
    public String getIdentifier() {
        return "MAX";
    }
}
