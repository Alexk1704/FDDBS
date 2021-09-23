package fdbs.sql.parser.ast.clause;

import fdbs.sql.parser.ast.expression.*;

/**
 * A sql statement WHERE clause.
 */
public class WhereClause extends Clause {

    private BinaryExpression binaryExpression;

    /**
     * The Constructor for a WHERE clause node.
     *
     * @param binaryExpression A binary expression node which represent a
     * single. or a nested binary expression.
     */
    public WhereClause(BinaryExpression binaryExpression) {
        super();

        this.binaryExpression = binaryExpression;
        this.addChild(this.binaryExpression);
    }

    /**
     * Gets the binary expression node which represent a single. or a nested
     * binary expression.
     *
     * @return Returns the binary expression node which represent a single. or a
     * nested binary expression.
     */
    public BinaryExpression getBinaryExpression() {
        return binaryExpression;
    }
}
