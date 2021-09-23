package fdbs.sql.parser.ast.function;

import fdbs.sql.parser.ast.identifier.*;

/**
 * A count function.
 */
public class CountFunction extends Function {

    private AllAttributeIdentifier allAttributeIdentifier;

    /**
     * The Constructor for a count function node.
     *
     * @param allAttributeIdentifier An all attribute identifier node which
     * represent the attribute name of the count function.
     */
    public CountFunction(AllAttributeIdentifier allAttributeIdentifier) {
        super();

        this.allAttributeIdentifier = allAttributeIdentifier;
        this.addChild(this.allAttributeIdentifier);
    }

    /**
     * Gets the all attribute identifier node which represent the attribute name
     * of the count function.
     *
     * @return Returns the all attribute identifier node which represent the
     * attribute name of the count function.
     */
    public AllAttributeIdentifier getAllAttributeIdentifier() {
        return allAttributeIdentifier;
    }
}
