package fdbs.sql.parser.ast.expression;

import fdbs.sql.parser.ast.ASTNode;

/**
 * An expression.
 */
public abstract class Expression extends ASTNode {

    /**
     * The Constructor for an expression node.
     */
    public Expression() {
        super();
    }

    /**
     * Gets the operator name.
     *
     * @return Returns the the operator name.
     */
    public abstract String getOperatorName();
}
