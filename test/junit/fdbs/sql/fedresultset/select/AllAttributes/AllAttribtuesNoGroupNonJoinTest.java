package junit.fdbs.sql.fedresultset.select.AllAttributes;

import fdbs.sql.*;
import fdbs.sql.meta.*;
import fdbs.util.logger.Logger;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;
import junit.fdbs.sql.fedresultset.FedResultSetTest;
import junit.fdbs.sql.fedresultset.select.*;
import org.junit.Test;
import static org.junit.Assert.*;

public class AllAttribtuesNoGroupNonJoinTest extends FedResultSetTest {

    // Definiere Where Tests
    public static List<NonJoinInfo> getNonJoinInfos() {
        return Arrays.asList(
                // no row
                new NonJoinInfo(metadataManager, "FLUG", "FNR", ">", "92", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) > 92;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "FNR", ">", "100", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) > 100;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                // one row
                new NonJoinInfo(metadataManager, "FLUG", "FNR", ">", "90", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) > 90;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "FNR", "=", "95", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) == 95;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                // multi rows
                new NonJoinInfo(metadataManager, "FLUG", "FNR", "<", "95", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) < 95;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "FLC", "=", "'DB'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(2).equals("DB");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "FLC", "=", "'AC'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(2).equals("AC");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "FLNR", "<=", "20", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(3) <= 20;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "VON", ">=", "'KLU'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(4).compareTo("KLU") >= 0;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "NACH", "!=", "'FRA'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = !row.getString(5).equals("FRA");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "AB", "<", "900", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(6) < 900;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NonJoinInfo(metadataManager, "FLUG", "AN", ">", "1250", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(7) > 1250;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                })
        );
    }

    public static List<NestedNonJoinInfos> getNestedOrNonJoinInfos() {
        return Arrays.asList(
                // no row
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", ">", "90000", "OR", "FLUG", "AB", "<", "0", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) > 90000 || row.getInt(6) < 0;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", ">", "90000", "OR", "FLUG", "AB", "<", "0", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) > 90000 || row.getInt(6) < 0;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                // one row
                new NestedNonJoinInfos(metadataManager, "FLUG", "FLC", "=", "'AC'", "OR", "FLUG", "AN", "<", "0", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(2).equals("AC") || row.getInt(7) < 0;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "VON", "=", "'FRA'", "OR", "FLUG", "AN", "<", "0", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(4).equals("FRA") || row.getInt(7) < 0;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                // two rows
                new NestedNonJoinInfos(metadataManager, "FLUG", "NACH", "=", "'YYZ'", "OR", "FLUG", "NACH", "=", "'YYZ'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(5).equals("AC") || row.getString(5).equals("YYZ");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", "=", "7", "OR", "FLUG", "AB", "=", "0", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) == 7 || row.getInt(6) == 0;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                //mulit rows
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", "=", "7", "OR", "FLUG", "NACH", "=", "'FRA'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) == 7 || row.getString(5).equals("FRA");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                //mulit rows
                new NestedNonJoinInfos(metadataManager, "FLUG", "FLC", "!=", "'BA'", "OR", "FLUG", "AB", "<", "1310", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = !row.getString(2).equals("BA") || row.getInt(6) < 1310;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "FLNR", ">", "50", "OR", "FLUG", "AN", "!=", "700", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(3) > 50 || row.getInt(7) != 700;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "VON", "<", "'K'", "OR", "FLUG", "FNR", "<", "10", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(4).compareTo("K") <= -1 || row.getInt(1) < 10;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", "<", "10", "OR", "FLUG", "FNR", ">", "90", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) < 10 || row.getInt(1) > 90;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "FLC", "=", "'AC'", "OR", "FLUG", "FLC", "!=", "'BA'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(2).equals("AC") || !row.getString(2).equals("BA");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                })
        );
    }

    public static List<NestedNonJoinInfos> getNestedAndNonJoinInfos() {
        return Arrays.asList(
                // no row same db
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", ">", "90000", "AND", "FLUG", "FNR", "<", "0", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) > 90000 && row.getInt(1) < 0;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                // no row no same db
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", ">", "90000", "AND", "FLUG", "NACH", "=", "'K'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) > 90000 && row.getString(5).equals("K");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                // one row same db
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", "=", "67", "AND", "FLUG", "FNR", "=", "67", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) == 67 && row.getInt(1) == 67;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", "=", "94", "AND", "FLUG", "FLC", "=", "'AC'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) == 94 && row.getString(2).equals("AC");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                // one row not same db
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", "=", "67", "AND", "FLUG", "AN", "=", "1230", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) == 67 && row.getInt(7) == 1230;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "FLC", "=", "'AF'", "AND", "FLUG", "NACH", "=", "'FRA'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(2).equals("AF") && row.getString(5).equals("FRA");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                // multi rows
                new NestedNonJoinInfos(metadataManager, "FLUG", "FNR", "<", "20", "AND", "FLUG", "FNR", ">", "20", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(1) < 20 && row.getInt(1) > 20;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "FLC", "=", "'DB'", "AND", "FLUG", "AB", "<", "1000", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(2).equals("DB") && row.getInt(6) < 1000;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }), new NestedNonJoinInfos(metadataManager, "FLUG", "AB", "<", "1000", "AND", "FLUG", "AN", ">", "1000", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getInt(6) < 1000 && row.getInt(7) > 1000;
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                }),
                new NestedNonJoinInfos(metadataManager, "FLUG", "VON", ">", "'A'", "AND", "FLUG", "NACH", "=", "'FRA'", (row) -> {
                    boolean useRow = false;
                    try {
                        useRow = row.getString(4).compareTo("A") >= 1 && row.getString(5).equals("FRA");
                    } catch (FedException ex) {
                        java.util.logging.Logger.getLogger(AllAttributeNoGrouptResultSetTest.class.getName()).log(Level.SEVERE, null, ex);
                        fail();
                    }
                    return useRow;
                })
        );
    }

    public static String createWhereClause(NestedNonJoinInfos info) {
        return String.format("WHERE ((%s.%s %s %s) %s (%s.%s %s %s))",
                info.getTable1Name(), info.getAttribute1Name(), info.getOperator1(), info.getConstant1(), info.getLinkingOperator(),
                info.getTable2Name(), info.getAttribute2Name(), info.getOperator2(), info.getConstant2());
    }

    public static String createWhereClause(NonJoinInfo info) {
        return String.format("WHERE ((%s.%s %s %s))", info.getTableName(), info.getAttributeName(), info.getOperator(), info.getConstant());
    }

    protected void testSelectWithEmptyList(Class c, String whereClause) throws FedException {
        try (FedStatement statement = executeQuery(String.format("SELECT * FROM FLUG %s", whereClause).trim());
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

            assertEquals(7, rs.getColumnCount());
            rs.getColumnName(1);
            rs.getColumnType(1);
        }
    }

    protected void testSelect(boolean doInserts, Class c, String whereClause, Predicate<? super FlugRow> whereFilterFunction) throws FedException {
        if (doInserts) {
            insertFLUG();
        }
        Table table = metadataManager.getTable("FLUG");
        int primaryKeyIndex = 1;

        for (Column column : table.getColumns()) {
            if (column.getAttributeName().equals(table.getPrimaryKeyConstraints().get(0).getAttributeName())) {
                break;
            }
            primaryKeyIndex++;
        }

        List<FlugRow> flugRows = getFlugRows();
        if (whereFilterFunction != null) {
            flugRows = flugRows.stream().filter(whereFilterFunction).collect(Collectors.toList());;
        }

        try (FedStatement statement = executeQuery(String.format("SELECT * FROM FLUG %s", whereClause).trim());
                FedResultSet rs = statement.getResultSet();) {
            assertTrue(c.isInstance(rs));

            rs.getColumnName(1);
            rs.getColumnType(1);
            assertEquals(7, rs.getColumnCount());

            int rowCount = 0;

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

            HashMap<Integer, FlugRow> flugRowsAsPKHashMap = getFlugRowsAsPKHashMap();

            while (rs.next() && rowCount < flugRowsAsPKHashMap.size()) {
                //get Flug by primary key FNR
                FlugRow flug = flugRowsAsPKHashMap.get(rs.getInt(primaryKeyIndex));

                //int FNR
                assertEquals(flug.getString(1), rs.getString(1));
                assertEquals(flug.getInt(1), rs.getInt(1));
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(1), rs.getColumnName(1));
                assertEquals(flug.getColumnType(1), rs.getColumnType(1));

                //String FLC;               
                assertEquals(flug.getString(2), rs.getString(2));
                boolean rsFailed = false;
                boolean flugFailed = false;
                try {
                    rs.getInt(2);
                } catch (FedException ex) {
                    rsFailed = true;
                }
                try {
                    flug.getInt(2);
                } catch (Exception ex) {
                    flugFailed = true;
                }
                assertEquals(rsFailed, flugFailed);

                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(2), rs.getColumnName(2));
                assertEquals(flug.getColumnType(2), rs.getColumnType(2));

                // int FLNR;
                assertEquals(flug.getString(3), rs.getString(3));
                assertEquals(flug.getInt(3), rs.getInt(3));
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(3), rs.getColumnName(3));
                assertEquals(flug.getColumnType(3), rs.getColumnType(3));

                //String VON;
                rsFailed = false;
                flugFailed = false;
                assertEquals(flug.getString(4), rs.getString(4));
                try {
                    rs.getInt(4);
                } catch (FedException ex) {
                    rsFailed = true;
                }
                try {
                    flug.getInt(4);
                } catch (Exception ex) {
                    flugFailed = true;
                }
                assertEquals(rsFailed, flugFailed);
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(4), rs.getColumnName(4));
                assertEquals(flug.getColumnType(4), rs.getColumnType(4));

                //String NACH;
                rsFailed = false;
                flugFailed = false;
                assertEquals(flug.getString(5), rs.getString(5));
                try {
                    rs.getInt(5);
                } catch (FedException ex) {
                    rsFailed = true;
                }
                try {
                    flug.getInt(5);
                } catch (Exception ex) {
                    flugFailed = true;
                }
                assertEquals(rsFailed, flugFailed);
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(5), rs.getColumnName(5));
                assertEquals(flug.getColumnType(5), rs.getColumnType(5));

                //int AB;
                assertEquals(flug.getString(6), rs.getString(6));
                assertEquals(flug.getInt(6), rs.getInt(6));
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(6), rs.getColumnName(6));
                assertEquals(flug.getColumnType(6), rs.getColumnType(6));

                // int AN;
                assertEquals(flug.getString(7), rs.getString(7));
                assertEquals(flug.getInt(7), rs.getInt(7));
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(7), rs.getColumnName(7));
                assertEquals(flug.getColumnType(7), rs.getColumnType(7));

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

            rs.getColumnName(1);
            rs.getColumnType(1);
            assertEquals(7, rs.getColumnCount());
        }
    }

    protected void testVonUnqieSelect(boolean doInserts, Class c, String whereClause, Predicate<? super FlugRow> whereFilterFunction) throws FedException {
        if (doInserts) {
            insertFlugVonUnqieRows();
        }

        Table table = metadataManager.getTable("FLUG");
        int primaryKeyIndex = 1;

        for (Column column : table.getColumns()) {
            if (column.getAttributeName().equals(table.getPrimaryKeyConstraints().get(0).getAttributeName())) {
                break;
            }
            primaryKeyIndex++;
        }

        List<FlugRow> flugRows = getFlugVonUnqieRows();
        if (whereFilterFunction != null) {
            flugRows = flugRows.stream().filter(whereFilterFunction).collect(Collectors.toList());;
        }

        try (FedStatement statement = executeQuery(String.format("SELECT * FROM FLUG %s", whereClause).trim());
                FedResultSet rs = statement.getResultSet();) {
            assertTrue(c.isInstance(rs));
            rs.getColumnName(1);
            rs.getColumnType(1);
            assertEquals(7, rs.getColumnCount());

            int rowCount = 0;

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

            HashMap<String, FlugRow> flugRowsAsPKHashMap = getFlugVonUnqieRowsAsPKHashMap();

            while (rs.next() && rowCount < flugRowsAsPKHashMap.size()) {
                //get Flug by primary key FNR
                FlugRow flug = flugRowsAsPKHashMap.get(rs.getString(primaryKeyIndex));

                //int FNR
                assertEquals(flug.getString(1), rs.getString(1));
                assertEquals(flug.getInt(1), rs.getInt(1));
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(1), rs.getColumnName(1));
                assertEquals(flug.getColumnType(1), rs.getColumnType(1));

                //String FLC;               
                assertEquals(flug.getString(2), rs.getString(2));
                boolean rsFailed = false;
                boolean flugFailed = false;
                try {
                    rs.getInt(2);
                } catch (FedException ex) {
                    rsFailed = true;
                }
                try {
                    flug.getInt(2);
                } catch (Exception ex) {
                    flugFailed = true;
                }
                assertEquals(rsFailed, flugFailed);

                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(2), rs.getColumnName(2));
                assertEquals(flug.getColumnType(2), rs.getColumnType(2));

                // int FLNR;
                assertEquals(flug.getString(3), rs.getString(3));
                assertEquals(flug.getInt(3), rs.getInt(3));
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(3), rs.getColumnName(3));
                assertEquals(flug.getColumnType(3), rs.getColumnType(3));

                //String VON;
                rsFailed = false;
                flugFailed = false;
                assertEquals(flug.getString(4), rs.getString(4));
                try {
                    rs.getInt(4);
                } catch (FedException ex) {
                    rsFailed = true;
                }
                try {
                    flug.getInt(4);
                } catch (Exception ex) {
                    flugFailed = true;
                }
                assertEquals(rsFailed, flugFailed);
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(4), rs.getColumnName(4));
                assertEquals(flug.getColumnType(4), rs.getColumnType(4));

                //String NACH;
                rsFailed = false;
                flugFailed = false;
                assertEquals(flug.getString(5), rs.getString(5));
                try {
                    rs.getInt(5);
                } catch (FedException ex) {
                    rsFailed = true;
                }
                try {
                    flug.getInt(5);
                } catch (Exception ex) {
                    flugFailed = true;
                }
                assertEquals(rsFailed, flugFailed);
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(5), rs.getColumnName(5));
                assertEquals(flug.getColumnType(5), rs.getColumnType(5));

                //int AB;
                assertEquals(flug.getString(6), rs.getString(6));
                assertEquals(flug.getInt(6), rs.getInt(6));
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(6), rs.getColumnName(6));
                assertEquals(flug.getColumnType(6), rs.getColumnType(6));

                // int AN;
                assertEquals(flug.getString(7), rs.getString(7));
                assertEquals(flug.getInt(7), rs.getInt(7));
                assertEquals(7, rs.getColumnCount());
                assertEquals(flug.getColumnName(7), rs.getColumnName(7));
                assertEquals(flug.getColumnType(7), rs.getColumnType(7));

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
                    rs.getColumnType(10);
                    fail();
                } catch (FedException ex) {
                    assertTrue(true); //Invalid Index
                }

                rowCount++;
            }

            Logger.infoln("Selected rows: " + rowCount);
            assertEquals(flugRows.size(), rowCount);

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

            rs.getColumnName(1);
            rs.getColumnType(1);
            assertEquals(7, rs.getColumnCount());
        }
    }
}
