package fdbs.sql.meta;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Constraint {

    @Override
    public String toString() {
        return "Constraint{" + "id=" + id + ", db=" + db + ", name=" + name + ", type=" + type + ", attributeName=" + attributeName + ", tableReference=" + tableReference + ", tableReferenceAttribute=" + tableReferenceAttribute + '}';
    }
    private int id;
    private int db;
    private String name;
    private ConstraintType type;
    private String attributeName;
    private String tableReference;
    private String tableReferenceAttribute;

    public Constraint(ResultSet constraint) throws SQLException {
        // fill the private variables with result from sql-query
        String type = constraint.getString("constraint_type");
        switch (type) {
            case "P":
                this.type = ConstraintType.PrimaryKey;
                break;
            case "F":
                this.type = ConstraintType.ForeignKey;
                break;
            case "U":
                this.type = ConstraintType.Unique;
                break;
            case "CNN":
                this.type = ConstraintType.CheckNull;
                break;
            case "CB":
                this.type = ConstraintType.CheckBetween;
                break;
            case "CC":
                this.type = ConstraintType.CheckComparison;
                break;
            default:
                throw new SQLException(String.format("%s: Unsupported ContraintType.", this.getClass().getSimpleName()));
        }

        id = constraint.getInt("table_id");
        db = constraint.getInt("database_id");
        name = constraint.getString("constraint_name");
        attributeName = constraint.getString("constraint_attribute");
        tableReference = constraint.getString("constraint_table_ref");
        tableReferenceAttribute = constraint.getString("constraint_table_ref_attribute");
    }

    public int getId() {
        return id;
    }

    public int getDb() {
        return db;
    }

    public String getName() {
        return name;
    }

    public ConstraintType getType() {
        return type;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getTableReference() {
        return tableReference;
    }

    public String getTableReferenceAttribute() {
        return tableReferenceAttribute;
    }
}
