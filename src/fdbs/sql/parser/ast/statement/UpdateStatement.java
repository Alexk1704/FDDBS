package fdbs.sql.parser.ast.statement;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.literal.*;
import fdbs.sql.parser.ast.clause.*;

/**
 * A sql update statement.
 */
public class UpdateStatement extends Statement {

    private TableIdentifier tableIdentifier;
    private AttributeIdentifier attributeIdentifier;
    private Literal constant;
    private WhereClause whereClause;

    /**
     * The Constructor for a sql update statement.
     *
     * @param tableIdentifier A table identifier node which represent the table
     * name of this update statement.
     * @param attributeIdentifier A attribute identifier node which represent
     * the attribute name of this update statement.
     * @param constant A constant node which represent the attribute value of
     * this update statement.
     * @param whereClause A where clause node which represent the where clause
     * of this update statement.
     */
    public UpdateStatement(TableIdentifier tableIdentifier, AttributeIdentifier attributeIdentifier, Literal constant, WhereClause whereClause) {
        super();

        this.tableIdentifier = tableIdentifier;
        this.addChild(this.tableIdentifier);

        this.attributeIdentifier = attributeIdentifier;
        this.addChild(this.attributeIdentifier);

        this.constant = constant;
        this.addChild(this.constant);

        if (whereClause != null) {
            this.whereClause = whereClause;
            this.addChild(this.whereClause);
        }
    }

    /**
     * Gets the table identifier node which represent the table name of this
     * update statement.
     *
     * @return Returns the table identifier node which represent the table name
     * of this update statement.
     */
    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    /**
     * Gets the attribute identifier node which represent the attribute name of
     * this update statement.
     *
     * @return Returns the attribute identifier node which represent the
     * attribute name of this update statement.
     */
    public AttributeIdentifier getAttributeIdentifier() {
        return attributeIdentifier;
    }

    /**
     * Gets the constant node which represent the attribute value of this update
     * statement.
     *
     * @return Returns the constant node which represent the attribute value of
     * this update statement.
     */
    public Literal getConstant() {
        return constant;
    }

    /**
     * Gets the where clause node which represent the where clause of this
     * update statement.
     *
     * @return Returns the where clause node which represent the where clause of
     * this update statement.
     */
    public WhereClause getWhereClause() {
        return whereClause;
    }
}
