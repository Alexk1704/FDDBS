package junit.fdbs.sql.fedresultset.select;

import fdbs.sql.FedException;
import java.sql.Types;

public class FlugRow {

    int FNR;
    String FLC;
    int FLNR;
    String VON;
    String NACH;
    int AB;
    int AN;

    public FlugRow(int FNR, String FLC, int FLNR, String VON, String NACH, int AB, int AN) {
        this.FNR = FNR;
        this.FLC = FLC;
        this.FLNR = FLNR;
        this.VON = VON;
        this.NACH = NACH;
        this.AB = AB;
        this.AN = AN;
    }

    public String getString(int columnIndex) throws FedException {
        String value;
        switch (columnIndex) {
            case 1:
                value = Integer.toString(FNR);
                break;
            case 2:
                value = FLC;
                break;
            case 3:
                value = Integer.toString(FLNR);
                break;
            case 4:
                value = VON;
                break;
            case 5:
                value = NACH;
                break;
            case 6:
                value = Integer.toString(AB);
                break;
            case 7:
                value = Integer.toString(AN);
                break;
            default:
                throw new FedException("");
        }

        return value;
    }

    public int getInt(int columnIndex) throws FedException {
        int value;
        switch (columnIndex) {
            case 1:
                value = FNR;
                break;
            case 2:
                value = Integer.getInteger(FLC);
                break;
            case 3:
                value = FLNR;
                break;
            case 4:
                value = Integer.getInteger(VON);
                break;
            case 5:
                value = Integer.getInteger(NACH);
                break;
            case 6:
                value = AB;
                break;
            case 7:
                value = AN;
                break;
            default:
                throw new FedException("");
        }

        return value;
    }

    public int getColumnCount() {
        return 7;
    }

    public String getColumnName(int columnIndex) throws FedException {
        String name;
        switch (columnIndex) {
            case 1:
                name = "FNR";
                break;
            case 2:
                name = "FLC";
                break;
            case 3:
                name = "FLNR";
                break;
            case 4:
                name = "VON";
                break;
            case 5:
                name = "NACH";
                break;
            case 6:
                name = "AB";
                break;
            case 7:
                name = "AN";
                break;
            default:
                throw new FedException("");
        }

        return name;
    }

    public int getColumnType(int columnIndex) throws FedException {
        int type;
        switch (columnIndex) {
            case 1:
                type = Types.NUMERIC;
                break;
            case 2:
                type = Types.VARCHAR;
                break;
            case 3:
                type = Types.NUMERIC;
                break;
            case 4:
                type = Types.VARCHAR;
                break;
            case 5:
                type = Types.VARCHAR;
                break;
            case 6:
                type = Types.NUMERIC;
                break;
            case 7:
                type = Types.NUMERIC;
                break;
            default:
                throw new FedException("");
        }

        return type;
    }
    
            
    public static int getColumnTypeByName(String columnName) throws FedException {
        int type;
        switch (columnName) {
            case "FNR":
                type = Types.NUMERIC;
                break;
            case "FLC":
                type = Types.VARCHAR;
                break;
            case "FLNR":
                type = Types.NUMERIC;
                break;
            case "VON":
                type = Types.VARCHAR;
                break;
            case "NACH":
                type = Types.VARCHAR;
                break;
            case "AB":
                type = Types.NUMERIC;
                break;
            case "AN":
                type = Types.NUMERIC;
                break;
            default:
                throw new FedException("");
        }

        return type;
    }
}
