package fdbs.sql.meta;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Column {

    @Override
    public String toString() {
        return "Column{" + "id=" + id + ", tableId=" + tableId + ", attributeName=" + attributeName + ", type=" + type + ", database=" + database + '}';
    }

    private int id;
    private int tableId;
    private String attributeName;
    private String type;
    private Integer database;
    private String defaultValue;

    public Column(ResultSet column) throws SQLException {
        // fill the private variables with result from sql-query
        id = column.getInt("column_id");
        tableId = column.getInt("table_id");
        attributeName = column.getString("column_name");
        type = column.getString("column_type");
        database = column.getInt("database_id");
        defaultValue = column.getString("column_default");
    }

    public int getId() {
        return id;
    }

    public int getTableId() {
        return tableId;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getType() {
        return type;
    }

    public Integer getDatabase() {
        return database;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }
}
