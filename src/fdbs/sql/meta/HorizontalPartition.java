package fdbs.sql.meta;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HorizontalPartition {

    @Override
    public String toString() {
        return "HorizontalPartition{" + "id=" + id + ", database=" + database + ", type=" + type + ", low=" + low + ", high=" + high + ", defaultValue=" + defaultValue + ", attributeName=" + attributeName + '}';
    }

    private int id;
    private int database;
    private String type;
    private String low;
    private String high;
    private String defaultValue;
    private String attributeName;

    public HorizontalPartition(ResultSet horizontal) throws SQLException {
        // fill the private variables with result from sql-query
        id = horizontal.getInt("horizontal_id");
        database = horizontal.getInt("database_id");
        type = horizontal.getString("attribute_type");
        attributeName = horizontal.getString("attribute_name");
        defaultValue = horizontal.getString("value_default");
        low = horizontal.getString("value_lowest");
        high = horizontal.getString("value_highest");
    }

    public int getId() {
        return id;
    }

    public int getDatabase() {
        return database;
    }

    public String getType() {
        return type;
    }

    public String getLow() {
        return low;
    }

    public String getHigh() {
        return high;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getAttributeName() {
        return attributeName;
    }

    /*
    
    The result is a negative integer if this String object lexicographically precedes the argument string. 
    The result is a positive integer if this String object lexicographically follows the argument string. 
    The result is zero if the strings are equal; 
    
    compareTo returns 0 exactly when the equals(Object) method would return true.   
     */
    public boolean compareValue(String sourceValue) {
        // compare if a sourceValue is a valid fit for this horizontal partitioning!
        int i;
        if (type.startsWith("VARCHAR")) {
    
            if (this.getLow() != null && this.getHigh() != null) {
                return sourceValue.compareToIgnoreCase(getLow()) > -1 && sourceValue.compareToIgnoreCase(getHigh()) <= 0;
            } else if (this.getLow() == null && this.getHigh() != null) {
                return sourceValue.compareToIgnoreCase(getHigh()) <= 0;
            } else if (this.getLow() != null && this.getHigh() == null) {
                return sourceValue.compareToIgnoreCase(getLow()) > -1;
            }

        } else {
            i = Integer.parseInt(sourceValue);
            if (this.getLow() != null && this.getHigh() != null) {
                return i >= Integer.parseInt(this.getLow()) && i <= Integer.parseInt(this.getHigh());
            } else if (this.getLow() == null && this.getHigh() != null) {
                return i <= Integer.parseInt(this.getHigh());
            } else if (this.getLow() != null && this.getHigh() == null) {
                return i >= Integer.parseInt(this.getLow());
            }

        }
        
        return false;
    }
}
