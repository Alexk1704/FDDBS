package fdbs.sql.meta;

import fdbs.sql.FedException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class Table {

    private int id;
    private String attributeName;
    private PartitionType partitionType;
    private ArrayList<Column> columns = new ArrayList();
    private LinkedHashMap<String, Column> columnsByName = new LinkedHashMap();
    private ArrayList<Constraint> constraints;
    private ArrayList<HorizontalPartition> horizontalPartitions;
    private ArrayList<Constraint> primaryKeyConstraints = new ArrayList<>();
    private HashMap<Integer, LinkedHashMap<String, Column>> databaseRelatedColumns = new HashMap<>();
    private HashMap<Integer, ArrayList<Constraint>> databaseRelatedConstraints = new HashMap<>();

    public Table(ResultSet table, ArrayList<Column> columns, ArrayList<Constraint> constraints, ArrayList<HorizontalPartition> horizontals) throws SQLException {
        // fill the private variables with result from sql-query
        id = table.getInt("table_id");
        attributeName = table.getString("table_name");
        String type = table.getString("partition_type");
        switch (type) {
            case "H":
                partitionType = PartitionType.Horizontal;
                break;
            case "V":
                partitionType = PartitionType.Vertical;
                break;
            default:
                partitionType = PartitionType.None;
                break;
        }

        this.columns = columns;
        this.constraints = constraints;
        this.horizontalPartitions = horizontals;

        for (Constraint constraint : this.constraints) {
            if (constraint.getType() == ConstraintType.PrimaryKey) {
                primaryKeyConstraints.add(constraint);
            }
        }

        LinkedHashMap<String, Column> db1 = new LinkedHashMap<>();
        LinkedHashMap<String, Column> db2 = new LinkedHashMap<>();
        LinkedHashMap<String, Column> db3 = new LinkedHashMap<>();
        this.columns.forEach((column) -> {
            if (column.getDatabase() == 1) {
                db1.put(column.getAttributeName(), column);
            } else if (column.getDatabase() == 2) {
                db2.put(column.getAttributeName(), column);
            } else if (column.getDatabase() == 3) {
                db3.put(column.getAttributeName(), column);
            } else if (column.getDatabase() == 0) {
                if (this.horizontalPartitions.size() == 3) {
                    db1.put(column.getAttributeName(), column);
                    db2.put(column.getAttributeName(), column);
                    db3.put(column.getAttributeName(), column);
                } else {
                    db1.put(column.getAttributeName(), column);
                    db2.put(column.getAttributeName(), column);
                }
            }

            this.columnsByName.put(column.getAttributeName(), column);
        });
        databaseRelatedColumns.put(1, db1);
        databaseRelatedColumns.put(2, db2);
        databaseRelatedColumns.put(3, db3);

        ArrayList<Constraint> constraintsDb1 = new ArrayList();
        ArrayList<Constraint> constraintsDb2 = new ArrayList();
        ArrayList<Constraint> constraintsDb3 = new ArrayList();

        this.constraints.forEach((constraint) -> {
            if (constraint.getDb() == 1 || constraint.getDb() == 0) {
                constraintsDb1.add(constraint);
            } else if (constraint.getDb() == 2) {
                constraintsDb2.add(constraint);
            } else if (constraint.getDb() == 3) {
                constraintsDb3.add(constraint);
            }
        });
        this.databaseRelatedConstraints.put(1, constraintsDb1);
        this.databaseRelatedConstraints.put(2, constraintsDb2);
        this.databaseRelatedConstraints.put(3, constraintsDb3);

    }

    public int getId() {
        return id;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public ArrayList<Constraint> getConstraints() {
        return constraints;
    }

    public ArrayList<HorizontalPartition> getHorizontalPartitions() {
        return horizontalPartitions;
    }

    public Column getColumn(String columnName) {
        return columnsByName.get(columnName);
    }

    public Column getColumn(int columnIndex) throws FedException {
        return getColumns().get(columnIndex);
    }

    public HashMap<String, Column> getColumnsByName() {
        return columnsByName;
    }

    public HashMap<Integer, ArrayList<Constraint>> getDatabaseRelatedConstraints() {
        return databaseRelatedConstraints;
    }

    /**
     * Holt Spalten anhand der DatenbankId.
     *
     * @param db
     * @return Eine HashMap der Columns der Datenbank.
     */
    public HashMap<String, Column> getColumnsByDatabase(int db) {
        return this.databaseRelatedColumns.get(db);
    }

    /**
     * Holt ein Constriant.
     *
     * @param db Die DatenbankId des Constraints.
     * @param constraintName Der Name des Constraints.
     * @return Das Constraint.
     */
    public Constraint getConstraint(int db, String constraintName) {
        for (Constraint constraint : constraints) {
            if (constraint.getName().equals(constraintName) && constraint.getDb() == db) {
                return constraint;
            }
        }

        return null;
    }

    /**
     * Holt Constraints der Datenbank.
     *
     * @param db Die DatenbankId.
     * @return Gibt eine Liste der Constraints zurück.
     */
    public ArrayList<Constraint> getConstraintsByDatabase(int db) {
        return this.databaseRelatedConstraints.get(db);
    }

    /**
     * Methode zum Holen der verschiedenen Primary Keys der Teiltabellen.
     *
     * @return Die PKs.
     */
    public ArrayList<Constraint> getPrimaryKeyConstraints() {
        return primaryKeyConstraints;
    }

    /**
     * Prüft, ob Spalte nullable ist.
     *
     * @param columnName Der Name der Spalte.
     * @return true oder false
     */
    public boolean isColumnNullAble(String columnName) {
        for (Constraint constraint : constraints) {
            if (constraint.getType() == ConstraintType.CheckNull && constraint.getAttributeName().equals(columnName)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Liefert die HashMap der DatabaseRelatedColumns.
     *
     * @return die DatabaseRelatedColumns.
     */
    public HashMap<Integer, LinkedHashMap<String, Column>> getDatabaseRelatedColumns() {
        return databaseRelatedColumns;
    }

    /**
     * Holt die vorkommenden Datenbanken einer Spalte.
     *
     * @param columnName Der Name der Spalte.
     * @return Eine Liste auf welche Datenbank diese Spalten vorkommen.
     */
    public List<Integer> getDatabasesForColumn(String columnName) {
        List<Integer> dbs = new ArrayList<>();
        HashMap<String, Column> columnsDB1 = getDatabaseRelatedColumns().get(1);
        HashMap<String, Column> columnsDB2 = getDatabaseRelatedColumns().get(2);
        HashMap<String, Column> columnsDB3 = getDatabaseRelatedColumns().get(3);

        if (columnsDB1.containsKey(columnName)) {
            dbs.add(1);
        }

        if (columnsDB2.containsKey(columnName)) {
            dbs.add(2);
        }

        if (columnsDB3.containsKey(columnName)) {
            dbs.add(3);
        }

        return dbs;
    }

    @Override
    public String toString() {
        return "Table{" + "id=" + id + ", attributeName=" + attributeName + ", partitionType=" + partitionType + ", columns=" + columns + ", constraints=" + constraints + ", horizontalPartitions=" + horizontalPartitions + '}';
    }
}
