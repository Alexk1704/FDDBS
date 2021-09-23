package junit.fdbs.sql.fedresultset.select;

import fdbs.sql.meta.Column;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.meta.Table;
import java.util.function.Predicate;

public class NestedNonJoinInfos {

    private MetadataManager manager;

    private String tableName1;
    private String attributeName1;
    private String operator1;
    private String constrant1;

    private String linkingOperand;

    private String tableName2;
    private String attributeName2;
    private String operator2;
    private String constrant2;

    private Predicate<? super FlugRow> filterFunction;

    public NestedNonJoinInfos(MetadataManager manager, String tableName1, String attributeName1, String operator1, String constrant1, String linkingOperand,
            String tableName2, String attributeName2, String operator2, String constrant2, Predicate<? super FlugRow> filterFunction) {

        this.manager = manager;

        this.tableName1 = tableName1;
        this.attributeName1 = attributeName1;
        this.operator1 = operator1;
        this.constrant1 = constrant1;

        this.linkingOperand = linkingOperand;

        this.tableName2 = tableName2;
        this.attributeName2 = attributeName2;
        this.operator2 = operator2;
        this.constrant2 = constrant2;

        this.filterFunction = filterFunction;
    }

    public String getTable1Name() {
        return tableName1;
    }

    public String getAttribute1Name() {
        return attributeName1;
    }

    public String getOperator1() {
        return operator1;
    }

    public String getConstant1() {
        return constrant1;
    }

    public String getTable2Name() {
        return tableName2;
    }

    public String getAttribute2Name() {
        return attributeName2;
    }

    public String getOperator2() {
        return operator2;
    }

    public String getConstant2() {
        return constrant2;
    }

    public String getLinkingOperator() {
        return linkingOperand;
    }

    public Predicate<? super FlugRow> getFilterFunction() {
        return filterFunction;
    }

    public boolean isNonJoinSameDb() {
        Table table1 = manager.getTable(tableName1);
        Column column1 = table1.getColumn(attributeName1);
        Table table2 = manager.getTable(tableName2);
        Column column2 = table2.getColumn(attributeName2);
        return column1.getDatabase().equals(column2.getDatabase());
    }
}
