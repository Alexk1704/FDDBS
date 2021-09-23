package junit.fdbs.sql.fedresultset.select;

import fdbs.sql.meta.Column;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.meta.Table;
import java.util.function.Predicate;

public class NonJoinInfo {

    private MetadataManager manager;
    private String tableName;
    private String attributeName;
    private String operator;
    private String constrant;
    private Predicate<? super FlugRow> filterFunction;

    public NonJoinInfo(MetadataManager manager, String tableName, String attributeName, String operator, String constrant, Predicate<? super FlugRow> filterFunction) {
        this.manager = manager;
        this.tableName = tableName;
        this.attributeName = attributeName;
        this.operator = operator;
        this.constrant = constrant;
        this.filterFunction = filterFunction;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getOperator() {
        return operator;
    }

    public String getConstant() {
        return constrant;
    }

    public Predicate<? super FlugRow> getFilterFunction() {
        return filterFunction;
    }
}
