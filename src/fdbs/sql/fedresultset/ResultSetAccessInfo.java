package fdbs.sql.fedresultset;

import java.sql.ResultSet;

public class ResultSetAccessInfo {

    private final ResultSet resultSet;
    private final int columnIndex;

    /**
     * @param resultSet A result set object.
     * @param columnIndex A column index for accessing result set infos.
     */
    public ResultSetAccessInfo(ResultSet resultSet, int columnIndex) {
        this.resultSet = resultSet;
        this.columnIndex = columnIndex;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public int getColumnIndex() {
        return columnIndex;
    }
}
