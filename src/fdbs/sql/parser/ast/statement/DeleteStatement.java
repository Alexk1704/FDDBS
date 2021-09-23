package fdbs.sql.parser.ast.statement;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.clause.*;

/**
 * A sql delete statement.
 */
public class DeleteStatement extends Statement {

    private TableIdentifier tableIdentifier;
    private WhereClause whereClause;

    /**
     * The Constructor for a delete statement.
     *
     * @param tableIdentifier A table identifier node which represent the table
     * name of this delete statement.
     * @param whereClause A where clause node which represent the where clause
     * of this delete statement.
     */
    public DeleteStatement(TableIdentifier tableIdentifier, WhereClause whereClause) {
        super();

        this.tableIdentifier = tableIdentifier;
        this.addChild(this.tableIdentifier);

        if (whereClause != null) {
            this.whereClause = whereClause;
            this.addChild(this.whereClause);
        }
    }

    /**
     * Gets the table identifier node which represent the table name of this
     * delete statement.
     *
     * @return Returns the table identifier node which represent the table name
     * of this delete statement.
     */
    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    /**
     * Gets the where clause node which represent the where clause of this
     * delete statement.
     *
     * @return Returns the where clause node which represent the where clause of
     * this delete statement.
     */
    public WhereClause getWhereClause() {
        return whereClause;
    }
}
