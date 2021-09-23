package fdbs.sql.parser.ast.statement.select;

import java.util.List;
import java.util.ArrayList;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.clause.*;

/**
 * A no select group statement.
 */
public class SelectNoGroupStatement extends SelectStatement {

    private AllAttributeIdentifier allAttributeIdentifier = null;
    private List<FullyQualifiedAttributeIdentifier> fQattributeIdentifiers = new ArrayList<>();
    private List<TableIdentifier> tableIdentifiers;
    private WhereClause whereClause;

    /**
     *
     * @param fQattributeIdentifiers A collection of fully qualified attribute
     * identifier nodes which represent a collection of fully qualified
     * attribute name whithin select no group statement.
     * @param tableIdentifiers A collection of table identifier nodes which
     * represent the table names of this select no group statement
     * @param whereClause A where clause node which represent the where clause
     * of this select no group statement.
     */
    public SelectNoGroupStatement(List<FullyQualifiedAttributeIdentifier> fQattributeIdentifiers, List<TableIdentifier> tableIdentifiers, WhereClause whereClause) {
        super();

        this.fQattributeIdentifiers = fQattributeIdentifiers;
        this.addChilds(this.fQattributeIdentifiers);

        this.tableIdentifiers = tableIdentifiers;
        this.addChilds(this.tableIdentifiers);

        if (whereClause != null) {
            this.whereClause = whereClause;
            this.addChild(this.whereClause);
        }
    }

    /**
     *
     * @param allAttributeIdentifier A all attribute identifier node which
     * represent a all attribute wildcard whithin select no group statement.
     * @param tableIdentifiers A collection of table identifier nodes which
     * represent the table names of this select no group statement
     * @param whereClause A where clause node which represent the where clause
     * of this select no group statement.
     */
    public SelectNoGroupStatement(AllAttributeIdentifier allAttributeIdentifier, List<TableIdentifier> tableIdentifiers, WhereClause whereClause) {
        super();

        this.allAttributeIdentifier = allAttributeIdentifier;
        this.addChild(this.allAttributeIdentifier);

        this.tableIdentifiers = tableIdentifiers;
        this.addChilds(this.tableIdentifiers);

        if (whereClause != null) {
            this.whereClause = whereClause;
            this.addChild(this.whereClause);
        }
    }

    /**
     * Has the select no group statement a all attribute identifier node.
     *
     * @return Returns true when the select no group statement has a all
     * attribute identifier node.
     */
    public boolean hasAllAttributeIdentifier() {
        return allAttributeIdentifier != null;
    }

    /**
     * Gets the all attribute identifier node which represent a all attribute
     * wildcard whithin select no group statement.
     *
     * @return Returns the all attribute identifier node which represent a all
     * attribute wildcard whithin select no group statement.
     */
    public AllAttributeIdentifier getAllAttributeIdentifier() {
        return allAttributeIdentifier;
    }

    /**
     * Gets the collection of fully qualified attribute identifier nodes which
     * represent a collection of fully qualified attribute name whithin select
     * no group statement.
     *
     * @return Returns the collection of fully qualified attribute identifier
     * nodes which represent a collection of fully qualified attribute name
     * whithin select no group statement.
     */
    public List<FullyQualifiedAttributeIdentifier> getFQAttributeIdentifiers() {
        return fQattributeIdentifiers;
    }

    /**
     * Gets the collection of table identifier nodes which represent the table
     * names of this select no group statement
     *
     * @return Returns the collection of table identifier nodes which represent
     * the table names of this select no group statement
     */
    public List<TableIdentifier> getTableIdentifiers() {
        return tableIdentifiers;
    }

    /**
     * Gets the where clause node which represent the where clause of this
     * select no group statement.
     *
     * @return Returns the where clause node which represent the where clause of
     * this select no group statement.
     */
    public WhereClause getWhereClause() {
        return whereClause;
    }

    /**
     * Sets the where clause node which represent the where clause of this
     * select no group statement.
     * @param whereClause A where clause.
     */
    public void setWhereClause(WhereClause whereClause) {
        this.whereClause = whereClause;
        addChild(this.whereClause);
    }
}
