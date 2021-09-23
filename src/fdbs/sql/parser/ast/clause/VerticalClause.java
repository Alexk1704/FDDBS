package fdbs.sql.parser.ast.clause;

import java.util.ArrayList;
import java.util.List;

import fdbs.sql.parser.ast.identifier.*;

/**
 * A sql statement VERTICAL clause.
 */
public class VerticalClause extends Clause {

    private List<AttributeIdentifier> attributesForDB1;
    private List<AttributeIdentifier> attributesForDB2;
    private List<AttributeIdentifier> attributesForDB3 = new ArrayList<>();

    /**
     * The Constructor for a VERTICAL clause node.
     *
     * @param attributesForDB1 An attribute identifier node which represent the
     * attribute name for the first database.
     * @param attributesForDB2 An attribute identifier node which represent the
     * attribute name for the second database.
     * @param attributesForDB3 An attribute identifier node which represent the
     * attribute name for the third database.
     */
    public VerticalClause(List<AttributeIdentifier> attributesForDB1, List<AttributeIdentifier> attributesForDB2, List<AttributeIdentifier> attributesForDB3) {
        super();

        this.attributesForDB1 = attributesForDB1;
        this.addChilds(this.attributesForDB1);

        this.attributesForDB2 = attributesForDB2;
        this.addChilds(this.attributesForDB2);

        if (attributesForDB3 != null && !attributesForDB3.isEmpty()) {
            this.attributesForDB3 = attributesForDB3;
            this.addChilds(this.attributesForDB3);
        }
    }

    /**
     * Gets the attribute identifier node which represent the attribute name for
     * the first database.
     *
     * @return Returns the attribute identifier node which represent the
     * attribute name for the first database.
     */
    public List<AttributeIdentifier> getAttributesForDB1() {
        return attributesForDB1;
    }

    /**
     * Gets the attribute identifier node which represent the attribute name for
     * the second database.
     *
     * @return Returns the attribute identifier node which represent the
     * attribute name for the second database.
     */
    public List<AttributeIdentifier> getAttributesForDB2() {
        return attributesForDB2;
    }

    /**
     * Gets the attribute identifier node which represent the attribute name for
     * the third database.
     *
     * @return Returns the attribute identifier node which represent the
     * attribute name for the third database.
     */
    public List<AttributeIdentifier> getAttributesForDB3() {
        return attributesForDB3;
    }
}
