package fdbs.sql.fedresultset;

public class ColumnAccessInfo {

    private final int resultSetDbId;
    private final int resultSetIndex;

    public ColumnAccessInfo(int resultSetDbId, int resultSetIndex) {
        this.resultSetDbId = resultSetDbId;
        this.resultSetIndex = resultSetIndex;
    }

    public int getResultSetDbId() {
        return resultSetDbId;
    }

    public int getResultSetIndex() {
        return resultSetIndex;
    }
}
