package fdbs.sql.parser.ast.statement;

import java.util.List;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.literal.*;

/**
 * A sql insert statement.
 */
public class InsertStatement extends Statement {

    private TableIdentifier tableIdentifier;
    private List<Literal> values;

    /**
     * The Constructor for a insert statement.
     *
     * @param tableIdentifier A table identifier node which represent the table
     * name of this insert statement.
     * @param values A literal node collection which represent the values of
     * this insert statement.
     */
    public InsertStatement(TableIdentifier tableIdentifier, List<Literal> values) {
        super();

        this.tableIdentifier = tableIdentifier;
        this.addChild(this.tableIdentifier);

        this.values = values;
        this.addChilds(this.values);
    }

    /**
     * Gets the table identifier node which represent the table name of this
     * insert statement.
     *
     * @return Returns the table identifier node which represent the table name
     * of this insert statement.
     */
    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    /**
     * Gets the literal node collection which represent the values of this
     * insert statement.
     *
     * @return Returns the literal node collection which represent the values of
     * this insert statement.
     */
    public List<Literal> getValues() {
        return values;
    }
}
