package fdbs.sql.fedresultset;

import java.sql.ResultSetMetaData;

public class MetaDataAccessInfo {

    private final ResultSetMetaData metaData;
    private final int columnIndex;

    /**
     *
     * @param metaData A result set meta data object.
     * @param columnIndex A column index for accessing meta data infos.
     */
    public MetaDataAccessInfo(ResultSetMetaData metaData, int columnIndex) {
        this.metaData = metaData;
        this.columnIndex = columnIndex;

    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public ResultSetMetaData getMetaData() {
        return metaData;
    }

}
