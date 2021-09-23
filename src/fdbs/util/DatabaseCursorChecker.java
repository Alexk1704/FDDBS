package fdbs.util;

import java.util.ArrayList;

public class DatabaseCursorChecker {

    public static ArrayList<Class> openStatements = new ArrayList<>();
    public static ArrayList<Class> closedStatements = new ArrayList<>();

    public static ArrayList<Class> openResultSet = new ArrayList<>();
    public static ArrayList<Class> closedResultSet = new ArrayList<>();

    public static void reset() {
        openStatements = new ArrayList<>();
        closedStatements = new ArrayList<>();

        openResultSet = new ArrayList<>();
        closedResultSet = new ArrayList<>();
    }

    public static void openStatement(Class opendClass) {
        openStatements.add(opendClass);
    }

    public static void closeStatement(Class closedClass) {
        closedStatements.add(closedClass);
    }

    public static void openResultSet(Class opendClass) {
        openResultSet.add(opendClass);
    }

    public static void closeResultSet(Class closedClass) {
        closedResultSet.add(closedClass);
    }

    public static int getOpenedStatements() {
        return openStatements.size();
    }

    public static int getClosedStatements() {
        return closedStatements.size();
    }

    public static int getOpenedResultSet() {
        return openResultSet.size();
    }

    public static int getClosedResultSet() {
        return closedResultSet.size();
    }
}
