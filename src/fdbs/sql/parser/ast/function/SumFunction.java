package fdbs.sql.parser.ast.function;

import fdbs.sql.parser.ast.identifier.*;

/**
 * A sum function.
 */
public class SumFunction extends Function {

    private FullyQualifiedAttributeIdentifier fQAttributeIdentifier;

    /**
     * The Constructor for a sum function node.
     *
     * @param fQAttributeIdentifier A fully qualified attribute identifier node
     * which represent the attribute name of the sum function.
     */
    public SumFunction(FullyQualifiedAttributeIdentifier fQAttributeIdentifier) {
        super();

        this.fQAttributeIdentifier = fQAttributeIdentifier;
        this.addChild(this.fQAttributeIdentifier);
    }

    /**
     * Gets the fully qualified attribute identifier node which represent the
     * attribute name of the sum function.
     *
     * @return Returns the fully qualified attribute identifier node which
     * represent the attribute name of the sum function.
     */
    public FullyQualifiedAttributeIdentifier getFQAttributeIdentifier() {
        return fQAttributeIdentifier;
    }
    
    @Override
    public String getIdentifier() {
        return "SUM";
    }
}
