package fdbs.sql.fedresultset;

import java.util.ArrayList;
import java.util.HashMap;

import fdbs.sql.meta.Table;

public class ResultSetHelpInfo {

    private final HashMap<Integer, String> fqAttributesForEachDB;
    private final Table table;
    private final String stmtAfterFrom;
    private final ArrayList<String> params;

    public ResultSetHelpInfo(Table table, String stmtAfterFrom,
            ArrayList<String> params) {

        this.table = table;
        this.stmtAfterFrom = stmtAfterFrom;
        this.params = params;
        this.fqAttributesForEachDB = new HashMap<>();
    }

    public ResultSetHelpInfo(HashMap<Integer, String> fqAttributesForEachDB, Table table, String stmtAfterFrom,
            ArrayList<String> params) {

        this.fqAttributesForEachDB = fqAttributesForEachDB;
        this.table = table;
        this.stmtAfterFrom = stmtAfterFrom;
        this.params = params;
    }

    public HashMap<Integer, String> getFqAttributesForEachDB() {
        return fqAttributesForEachDB;
    }

    public Table getTable() {
        return table;
    }

    public String getStmtAfterFrom() {
        return stmtAfterFrom;
    }

    public ArrayList<String> getParams() {
        return params;
    }
}
