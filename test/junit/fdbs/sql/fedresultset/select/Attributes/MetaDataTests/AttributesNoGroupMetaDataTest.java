package junit.fdbs.sql.fedresultset.select.Attributes.MetaDataTests;

import fdbs.sql.FedException;
import fdbs.sql.FedResultSet;
import fdbs.sql.FedStatement;
import fdbs.util.logger.Logger;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import junit.fdbs.sql.fedresultset.FedResultSetTest;
import junit.fdbs.sql.fedresultset.select.FlugRow;
import static org.junit.Assert.*;

public class AttributesNoGroupMetaDataTest extends FedResultSetTest {

    public static List<List<String>> getAttributesVariations() {
        return Arrays.asList(
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.VON"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC"),
                Arrays.asList("FLUG.FNR"),
                Arrays.asList("FLUG.FLC", "FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FLNR", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.FLNR", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.AN"),
                Arrays.asList("FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.VON", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.NACH", "FLUG.AB", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC", "FLUG.AN"),
                Arrays.asList("FLUG.FNR", "FLUG.FLC"));
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

    protected void testSelectMetaData(boolean doInserts, Class c, List<String> fqAttributes, String whereClause, Predicate<? super FlugRow> whereFilterFunction) throws FedException {
        if (doInserts) {
            insertFLUG();
        }
        validateMetadata(getFlugRows(), c, fqAttributes, whereClause, whereFilterFunction);
    }

    protected void testFlugVonUnqieSelectMetaData(boolean doInserts, Class c, List<String> fqAttributes, String whereClause, Predicate<? super FlugRow> whereFilterFunction) throws FedException {
        if (doInserts) {
            insertFlugVonUnqieRows();
        }
        validateMetadata(getFlugVonUnqieRows(), c, fqAttributes, whereClause, whereFilterFunction);
    }

    protected void validateMetadata(List<FlugRow> usedFlugRows, Class c, List<String> fqAttributes, String whereClause, Predicate<? super FlugRow> whereFilterFunction) throws FedException {
        String selectStmt = String.format("SELECT %s FROM FLUG %s", String.join(",", fqAttributes), whereClause).trim();
        Logger.infoln("Test: " + selectStmt);
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

}
