package junit.fdbs.sql.fedresultset.select.Attributes.ValueTests;

import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.FedStatement;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.Table;
import fdbs.util.logger.Logger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import junit.fdbs.sql.fedresultset.select.AllAttributes.AllAttribtuesNoGroupNonJoinTest;
import junit.fdbs.sql.fedresultset.select.FlugRow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AttributesNoGroupValueTest extends AllAttribtuesNoGroupNonJoinTest {

    public static List<List<String>> getAttributesVariationsWithFNRPK() {
        return Arrays.asList(
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC"),
                Arrays.asList("FLUG.FNR"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC"),
                Arrays.asList("FLUG.FNR", "FLUG.VON", "FLUG.NACH", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.VON", "FLUG.NACH")
        );
    }

    public static List<List<String>> getAttributesVariationsWithVONPK() {
        return Arrays.asList(
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.VON", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.VON", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.VON", "FLUG.NACH", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB"),
                Arrays.asList("FLUG.FNR", "FLUG.VON", "FLUG.AB")
        );
    }

    protected void testSelectWithEmptyList(Class c, List<String> attributes, String whereClause) throws FedException {
        String selectStmt = String.format("SELECT %s FROM FLUG %s", String.join(",", attributes), whereClause).trim();
        Logger.infoln("Test: " + selectStmt);
        try (FedStatement statement = executeQuery(selectStmt);
                FedResultSet rs = statement.getResultSet();) {
            assertTrue(!rs.next());

            try {
                rs.getString(1);
                fail();
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getInt(1);
                fail();
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            assertEquals(attributes.size(), rs.getColumnCount());
            rs.getColumnName(1);
            rs.getColumnType(1);
        }
    }

    protected void testSelectData(boolean doInserts, Class c, List<String> fqAttributes, String whereClause, Predicate<? super FlugRow> whereFilterFunction) throws FedException {
        if (doInserts) {
            insertFLUG();
        }
        validateData(getFlugRows(), c, fqAttributes, whereClause, whereFilterFunction, "FLUG.FNR");
    }

    protected void testFlugVonUnqieSelectData(boolean doInserts, Class c, List<String> fqAttributes, String whereClause, Predicate<? super FlugRow> whereFilterFunction) throws FedException {
        if (doInserts) {
            insertFlugVonUnqieRows();
        }
        validateData(getFlugVonUnqieRows(), c, fqAttributes, whereClause, whereFilterFunction, "FLUG.VON");
    }

    protected void validateData(List<FlugRow> usedFlugRows, Class c, List<String> fqAttributes, String whereClause, Predicate<? super FlugRow> whereFilterFunction, String fqPKName) throws FedException {
        String selectStmt = String.format("SELECT %s FROM FLUG %s", String.join(",", fqAttributes), whereClause).trim();
        Logger.infoln("Test: " + selectStmt);

        Table table = metadataManager.getTable("FLUG");
        int primaryKeyIndex = 1;

        for (Column column : table.getColumns()) {
            if (column.getAttributeName().equals(table.getPrimaryKeyConstraints().get(0).getAttributeName())) {
                break;
            }
            primaryKeyIndex++;
        }

        if (primaryKeyIndex <= 0) {
            fail();
        }

        try (FedStatement statement = executeQuery(selectStmt);
                FedResultSet rs = statement.getResultSet();) {
            assertTrue(c.isInstance(rs));
            assertEquals(fqAttributes.size(), rs.getColumnCount());
            rs.getColumnName(1);
            rs.getColumnType(1);

            List<FlugRow> flugRows = usedFlugRows;
            if (whereFilterFunction != null) {
                flugRows = flugRows.stream().filter(whereFilterFunction).collect(Collectors.toList());;
            }

            HashMap<String, FlugRow> flugRowsAsPKHashMap = getFlugVRowsAsPKHashMap(flugRows, primaryKeyIndex);

            try {
                rs.getString(1);
                fail();
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getInt(1);
                fail();
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            int rowCount = 0;
            while (rs.next()) {

                FlugRow flug = flugRowsAsPKHashMap.get(rs.getString(fqAttributes.indexOf(fqPKName) + 1));

                //int FNR
                if (fqAttributes.contains("FLUG.FNR")) {
                    int attributeIndex = fqAttributes.indexOf("FLUG.FNR") + 1;
                    assertEquals(flug.getString(1), rs.getString(attributeIndex));
                    assertEquals(flug.getInt(1), rs.getInt(attributeIndex));
                    assertEquals(fqAttributes.size(), rs.getColumnCount());
                    assertEquals(flug.getColumnName(1), rs.getColumnName(attributeIndex));
                    assertEquals(flug.getColumnType(1), rs.getColumnType(attributeIndex));
                }

                //String FLC;       
                if (fqAttributes.contains("FLUG.FLC")) {
                    int attributeIndex = fqAttributes.indexOf("FLUG.FLC") + 1;
                    try {
                        assertEquals(flug.getString(2), rs.getString(attributeIndex));
                    } catch (Exception ex) {
                        throw ex;
                    }

                    boolean rsFailed = false;
                    boolean flugFailed = false;
                    try {
                        rs.getInt(attributeIndex);
                    } catch (FedException ex) {
                        rsFailed = true;
                    }
                    try {
                        flug.getInt(2);
                    } catch (Exception ex) {
                        flugFailed = true;
                    }
                    assertEquals(rsFailed, flugFailed);

                    assertEquals(fqAttributes.size(), rs.getColumnCount());
                    assertEquals(flug.getColumnName(2), rs.getColumnName(attributeIndex));
                    assertEquals(flug.getColumnType(2), rs.getColumnType(attributeIndex));
                }

                // int FLNR;
                if (fqAttributes.contains("FLUG.FLNR")) {
                    int attributeIndex = fqAttributes.indexOf("FLUG.FLNR") + 1;
                    assertEquals(flug.getString(3), rs.getString(attributeIndex));
                    assertEquals(flug.getInt(3), rs.getInt(attributeIndex));
                    assertEquals(fqAttributes.size(), rs.getColumnCount());
                    assertEquals(flug.getColumnName(3), rs.getColumnName(attributeIndex));
                    assertEquals(flug.getColumnType(3), rs.getColumnType(attributeIndex));
                }

                //String VON;
                if (fqAttributes.contains("FLUG.VON")) {
                    int attributeIndex = fqAttributes.indexOf("FLUG.VON") + 1;
                    boolean rsFailed = false;
                    boolean flugFailed = false;
                    assertEquals(flug.getString(4), rs.getString(attributeIndex));
                    try {
                        rs.getInt(attributeIndex);
                    } catch (FedException ex) {
                        rsFailed = true;
                    }
                    try {
                        flug.getInt(4);
                    } catch (Exception ex) {
                        flugFailed = true;
                    }
                    assertEquals(rsFailed, flugFailed);
                    assertEquals(fqAttributes.size(), rs.getColumnCount());
                    assertEquals(flug.getColumnName(4), rs.getColumnName(attributeIndex));
                    assertEquals(flug.getColumnType(4), rs.getColumnType(attributeIndex));
                }

                //String NACH;
                if (fqAttributes.contains("FLUG.NACH")) {
                    int attributeIndex = fqAttributes.indexOf("FLUG.NACH") + 1;
                    boolean rsFailed = false;
                    boolean flugFailed = false;
                    assertEquals(flug.getString(5), rs.getString(attributeIndex));
                    try {
                        rs.getInt(attributeIndex);
                    } catch (FedException ex) {
                        rsFailed = true;
                    }
                    try {
                        flug.getInt(5);
                    } catch (Exception ex) {
                        flugFailed = true;
                    }
                    assertEquals(rsFailed, flugFailed);
                    assertEquals(fqAttributes.size(), rs.getColumnCount());
                    assertEquals(flug.getColumnName(5), rs.getColumnName(attributeIndex));
                    assertEquals(flug.getColumnType(5), rs.getColumnType(attributeIndex));
                }

                //int AB;
                if (fqAttributes.contains("FLUG.AB")) {
                    int attributeIndex = fqAttributes.indexOf("FLUG.AB") + 1;
                    assertEquals(flug.getString(6), rs.getString(attributeIndex));
                    assertEquals(flug.getInt(6), rs.getInt(attributeIndex));
                    assertEquals(fqAttributes.size(), rs.getColumnCount());
                    assertEquals(flug.getColumnName(6), rs.getColumnName(attributeIndex));
                    assertEquals(flug.getColumnType(6), rs.getColumnType(attributeIndex));
                }

                // int AN;
                if (fqAttributes.contains("FLUG.AN")) {
                    int attributeIndex = fqAttributes.indexOf("FLUG.AN") + 1;
                    assertEquals(flug.getString(7), rs.getString(attributeIndex));
                    assertEquals(flug.getInt(7), rs.getInt(attributeIndex));
                    assertEquals(fqAttributes.size(), rs.getColumnCount());
                    assertEquals(flug.getColumnName(7), rs.getColumnName(attributeIndex));
                    assertEquals(flug.getColumnType(7), rs.getColumnType(attributeIndex));
                }

                try {
                    rs.getString(0);
                    fail();
                } catch (FedException ex) {
                    assertTrue(true); //Invalid Index
                }

                try {
                    rs.getString(8);
                    fail();
                } catch (FedException ex) {
                    assertTrue(true); //Invalid Index
                }

                try {
                    rs.getInt(8);
                    fail();
                } catch (FedException ex) {
                    assertTrue(true); //Invalid Index
                }

                try {
                    rs.getColumnName(0);
                    fail();
                } catch (FedException ex) {
                    assertTrue(true); //Invalid Index
                }

                try {
                    rs.getColumnType(10);
                    fail();
                } catch (FedException ex) {
                    assertTrue(true); //Invalid Index
                }

                rowCount++;
            }

            Logger.infoln("Selectd rows: " + rowCount);
            assertEquals(flugRows.size(), rowCount);

            assertEquals(fqAttributes.size(), rs.getColumnCount());

            for (int i = 1; i <= fqAttributes.size(); i++) {
                String fqAttribute = fqAttributes.get(i - 1);
                String attributeName = fqAttribute.split("\\.")[1];
                assertEquals(attributeName, rs.getColumnName(i));
                assertEquals(FlugRow.getColumnTypeByName(attributeName), rs.getColumnType(i));
            }

            try {
                rs.getString(0);
                fail();
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getString(fqAttributes.size() + 1);
                fail();
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getInt(fqAttributes.size() + 1);
                fail();
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getColumnName(0);
                fail();
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            try {
                rs.getColumnType(10);
                fail();
            } catch (FedException ex) {
                assertTrue(true); //Invalid Index
            }

            assertTrue(!rs.next()); // rs.next() == false

            try {
                rs.getString(1);
                fail();
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            try {
                rs.getInt(1);
                fail();
            } catch (FedException ex) {
                assertTrue(true); // No current row
            }

            assertEquals(fqAttributes.size(), rs.getColumnCount());
            rs.getColumnName(1);
            rs.getColumnType(1);
        }
    }

    protected HashMap<String, FlugRow> getFlugVRowsAsPKHashMap(List<FlugRow> flugRows, int pkIndex) throws FedException {
        HashMap<String, FlugRow> map = new HashMap<>();
        for (FlugRow flug : flugRows) {
            map.put(flug.getString(pkIndex), flug);
        }

        return map;
    }

}
