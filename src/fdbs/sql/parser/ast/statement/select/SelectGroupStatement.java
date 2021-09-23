package fdbs.sql.parser.ast.statement.select;

import java.util.List;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.clause.*;
import fdbs.sql.parser.ast.function.*;

/**
 * A select group statement.
 */
public class SelectGroupStatement extends SelectStatement {

    private FullyQualifiedAttributeIdentifier fQattributeIdentifier;
    private Function function;
    private List<TableIdentifier> tableIdentifiers;
    private WhereClause whereClause;
    private GroupByClause groupByClause;
    private HavingClause havingClause;

    /**
     * The Constructor for a select group statement.
     *
     * @param fQattributeIdentifier A fully qualified attribute identifier node
     * which represent a fully qualified attribute name whithin select group
     * statement.
     * @param function A function node which represent a function whithin select
     * group statement.
     * @param tableIdentifiers A collection of table identifier nodes which
     * represent the table names of this select group statement
     * @param whereClause A where clause node which represent the where clause
     * of this select group statement.
     * @param groupByClause A group by clause node which represent the group by
     * clause of this select group statement.
     * @param havingClause A having clause node which represent the having
     * clause of this select group statement.
     */
    public SelectGroupStatement(FullyQualifiedAttributeIdentifier fQattributeIdentifier, Function function,
            List<TableIdentifier> tableIdentifiers, WhereClause whereClause, GroupByClause groupByClause, HavingClause havingClause) {
        super();

        this.fQattributeIdentifier = fQattributeIdentifier;
        this.addChild(this.fQattributeIdentifier);

        this.function = function;
        this.addChild(this.function);

        this.tableIdentifiers = tableIdentifiers;
        this.addChilds(this.tableIdentifiers);

        if (whereClause != null) {
            this.whereClause = whereClause;
            this.addChild(this.whereClause);
        }

        this.groupByClause = groupByClause;
        this.addChild(this.groupByClause);

        if (havingClause != null) {
            this.havingClause = havingClause;
            this.addChild(this.havingClause);
        }
    }

    /**
     * Gets the fully qualified attribute identifier node which represent a
     * fully qualified attribute name whithin select group statement.
     *
     * @return Returns the fully qualified attribute identifier node which
     * represent a fully qualified attribute name whithin select group
     * statement.
     */
    public FullyQualifiedAttributeIdentifier getFQAttributeIdentifier() {
        return fQattributeIdentifier;
    }

    /**
     * Gets the function node which represent a function whithin select group
     * statement.
     *
     * @return Returns the function node which represent a function whithin
     * select group statement.
     */
    public Function getFunction() {
        return function;
    }

    /**
     * Gets the collection of table identifier nodes which represent the table
     * names of this select group statement
     *
     * @return Returns the collection of table identifier nodes which represent
     * the table names of this select group statement
     */
    public List<TableIdentifier> getTableIdentifiers() {
        return tableIdentifiers;
    }

    /**
     * Gets the where clause node which represent the where clause of this
     * select group statement.
     *
     * @return Returns the where clause node which represent the where clause of
     * this select group statement.
     */
    public WhereClause getWhereClause() {
        return whereClause;
    }

    /**
     * Gets the group by clause node which represent the group by clause of this
     * select group statement.
     *
     * @return Returns the group by clause node which represent the group by
     * clause of this select group statement.
     */
    public GroupByClause getGroupByClause() {
        return groupByClause;
    }

    /**
     * Gets the having clause node which represent the having clause of this
     * select group statement.
     *
     * @return Returns the having clause node which represent the having clause
     * of this select group statement.
     */
    public HavingClause getHavingClause() {
        return havingClause;
    }
}
