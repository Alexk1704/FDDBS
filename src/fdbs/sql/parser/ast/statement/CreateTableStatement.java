package fdbs.sql.parser.ast.statement;

import java.util.List;

import fdbs.sql.parser.ast.identifier.*;
import fdbs.sql.parser.ast.attribute.*;
import fdbs.sql.parser.ast.clause.*;
import fdbs.sql.parser.ast.constraint.*;

/**
 * A sql create table statement.
 */
public class CreateTableStatement extends Statement {

    private TableIdentifier tableIdentifier;
    private List<TableAttribute> attributes;
    private List<Constraint> constraints;
    private HorizontalClause horizontalClause;
    private VerticalClause verticalClause;

    /**
     * The Constructor for a create table statement.
     *
     * @param tableIdentifier A table identifier node which represent the table
     * name of this create table statement.
     * @param attributes A collection of table attribute nodes which represent
     * the attributes of this table statment.
     * @param constraints A collection of constraints nodes which represent the
     * constraints of this table statment.
     */
    public CreateTableStatement(TableIdentifier tableIdentifier, List<TableAttribute> attributes, List<Constraint> constraints) {
        super();

        this.tableIdentifier = tableIdentifier;
        this.addChild(tableIdentifier);

        this.attributes = attributes;
        this.addChilds(this.attributes);

        this.constraints = constraints;
        this.addChilds(this.constraints);
    }

    /**
     * Sets the horizontal clause node which represent the horizontal clause of
     * this create table statement.
     *
     * @param horizontalClause The horizontal clause node which represent the
     * horizontal clause of this create table statement.
     */
    public void setHorizontalClause(HorizontalClause horizontalClause) {
        this.horizontalClause = horizontalClause;
        this.addChild(this.horizontalClause);
    }

    /**
     * Sets the vertical clause node which represent the vertical clause of this
     * create table statement.
     *
     * @param verticalClause The vertical clause node which represent the
     * vertical clause of this create table statement.
     */
    public void setVerticalClause(VerticalClause verticalClause) {
        this.verticalClause = verticalClause;
        this.addChild(this.verticalClause);
    }

    /**
     * Gets the table identifier node which represent the table name of this
     * create table statement.
     *
     * @return Returns the table identifier node which represent the table name
     * of this create table statement.
     */
    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    /**
     * Gets the collection of table attribute nodes which represent the
     * attributes of this table statment.
     *
     * @return Returns the collection of table attribute nodes which represent
     * the attributes of this table statment.
     */
    public List<TableAttribute> getAttributes() {
        return attributes;
    }

    /**
     * Gets the collection of constraints nodes which represent the constraints
     * of this table statment.
     *
     * @return Returns the collection of constraints nodes which represent the
     * constraints of this table statment.
     */
    public List<Constraint> getConstraints() {
        return constraints;
    }

    /**
     * Gets the horizontal clause node which represent the horizontal clause of
     * this create table statement.
     *
     * @return Returns the horizontal clause node which represent the horizontal
     * clause of this create table statement.
     */
    public HorizontalClause getHorizontalClause() {
        return horizontalClause;
    }

    /**
     * Gets the vertical clause node which represent the vertical clause of this
     * create table statement.
     *
     * @return Returns the vertical clause node which represent the vertical
     * clause of this create table statement.
     */
    public VerticalClause getVerticalClause() {
        return verticalClause;
    }
}
