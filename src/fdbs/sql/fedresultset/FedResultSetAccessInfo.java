package fdbs.sql.fedresultset;

import fdbs.sql.FedResultSet;

public class FedResultSetAccessInfo {

    private FedResultSet fedResultSet;
    private int columnIndex;

    public FedResultSetAccessInfo(FedResultSet fedResultSet, int columnIndex) {
        this.fedResultSet = fedResultSet;
        this.columnIndex = columnIndex;
    }

    public FedResultSet getResultSet() {
        return fedResultSet;
    }

    public int getColumnIndex() {
        return columnIndex;
    }
}
