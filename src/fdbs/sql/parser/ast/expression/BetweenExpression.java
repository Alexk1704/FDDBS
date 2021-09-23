package fdbs.sql.parser.ast.expression;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.literal.*;

/**
 * A between expression.
 */
public class BetweenExpression extends Expression {

    private AttributeIdentifier attributeIdentifier;
    private Literal min;
    private Literal max;

    /**
     * The Constructor for a between expression node.
     *
     * @param attributeIdentifier An attribute identifier node which represent
     * the attribute name of this between expression.
     * @param min A literal node which represent the minimal value.
     * @param max A literal node which represent the maximum value.
     */
    public BetweenExpression(AttributeIdentifier attributeIdentifier, Literal min, Literal max) {
        super();

        this.attributeIdentifier = attributeIdentifier;
        this.addChild(this.attributeIdentifier);

        this.min = min;
        this.addChild(this.min);

        this.max = max;
        this.addChild(this.max);
    }

    /**
     * Gets the attribute identifier node which represent the attribute name of
     * this between expression.
     *
     * @return Returns the attribute identifier node which represent the
     * attribute name of this between expression.
     */
    public AttributeIdentifier getAttributeIdentifier() {
        return attributeIdentifier;
    }

    /**
     * Gets the operator name.
     *
     * @return Returns the the operator name.
     */
    @Override
    public String getOperatorName() {
        return "BETWEEN";
    }

    /**
     * Gets the literal node which represent the minimal value.
     *
     * @return Returns the literal node which represent the minimal value.
     */
    public Literal getMin() {
        return min;
    }

    /**
     * Gets the literal node which represent the maximum value.
     *
     * @return Returns the literal node which represent the maximum value.
     */
    public Literal getMax() {
        return max;
    }
}
