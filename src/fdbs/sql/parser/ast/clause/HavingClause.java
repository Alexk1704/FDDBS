package fdbs.sql.parser.ast.clause;

import fdbs.sql.parser.ast.expression.*;

/**
 * A sql statement HAVING clause.
 */
public class HavingClause extends Clause {

    BinaryExpression countComparisonExpression;

    /**
     * The Constructor for a HAVING clause node.
     *
     * @param countComparisonExpression A count comparison expression node which
     * represent a COUNT() expression.
     */
    public HavingClause(BinaryExpression countComparisonExpression) {
        super();

        this.countComparisonExpression = countComparisonExpression;
        this.addChild(this.countComparisonExpression);
    }

    /**
     * Gets the count comparison expression node which represent a COUNT()
     * expression.
     *
     * @return Returns the count comparison expression node which represent a
     * COUNT() expression.
     */
    public BinaryExpression getCountComparisonExpression() {
        return countComparisonExpression;
    }

}
