package fdbs.sql.parser.ast.statement.select;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.function.*;

/**
 * A select count all table statement.
 */
public class SelectCountAllTableStatement extends SelectStatement {

    private CountFunction countFunction;
    private TableIdentifier tableIdentifier;

    /**
     * The Constructor for a select count all table statement.
     *
     * @param countFunction A count function node which represent the count
     * function of this select count all table statement.
     * @param tableIdentifier A table identifier node which represent the table
     * name of this select count all table statement.
     */
    public SelectCountAllTableStatement(CountFunction countFunction, TableIdentifier tableIdentifier) {
        super();

        this.countFunction = countFunction;
        this.addChild(this.countFunction);

        this.tableIdentifier = tableIdentifier;
        this.addChild(this.tableIdentifier);
    }

    /**
     * Gets the count function node which represent the count function of this
     * select count all table statement.
     *
     * @return Returns the count function node which represent the count
     * function of this select count all table statement.
     */
    public CountFunction getCountFunction() {
        return countFunction;
    }

    /**
     * Gets the table identifier node which represent the table name of this
     * select count all table statement.
     *
     * @return Returns the table identifier node which represent the table name
     * of this select count all table statement.
     */
    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }
}
