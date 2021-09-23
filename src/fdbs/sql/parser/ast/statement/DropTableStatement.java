package fdbs.sql.parser.ast.statement;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.extension.*;

/**
 * A sql drop table statement.
 */
public class DropTableStatement extends Statement {

    private TableIdentifier tableIdentifier;
    private CascadeConstraintsExtension cascadeConstraint = null;
    private PurgeExtension purge = null;

    /**
     * The Constructor for a drop statement.
     *
     * @param tableIdentifier A table identifier node which represent the table
     * name of this drop table statement.
     * @param cascadeConstraint A cascade constraint node which represent a
     * cascade constraint of this drop table statement.
     * @param purge A purge extension node which represent a purge extension of
     * this drop table statement.
     */
    public DropTableStatement(TableIdentifier tableIdentifier, CascadeConstraintsExtension cascadeConstraint,
            PurgeExtension purge) {
        super();

        this.tableIdentifier = tableIdentifier;
        this.addChild(this.tableIdentifier);

        if (cascadeConstraint != null) {
            this.cascadeConstraint = cascadeConstraint;
            this.addChild(this.cascadeConstraint);
        }

        if (purge != null) {
            this.purge = purge;
            this.addChild(this.purge);
        }
    }

    /**
     * Gets the identifier node which represent the table name of this drop
     * table statement.
     *
     * @return Returns the identifier node which represent the table name of
     * this drop table statement.
     */
    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    /**
     * Has the drop table statement a cascade constraint.
     *
     * @return Returns true when the drop table statement has a cascade
     * constraint.
     */
    public boolean hasCascadeConstraintsExtension() {
        return cascadeConstraint != null;
    }

    /**
     * Has the drop table statement a purge extension.
     *
     * @return Returns true when the drop table statement has a purge extension.
     */
    public boolean hasPurgeExtension() {
        return purge != null;
    }
}
