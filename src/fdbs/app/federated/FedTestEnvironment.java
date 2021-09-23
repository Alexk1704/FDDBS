package fdbs.app.federated;

import fdbs.sql.*;
import fdbs.util.logger.Logger;
import java.util.Scanner;

public class FedTestEnvironment {

    private FedConnection fedConnection;

    public FedTestEnvironment(final FedConnection fedConnection) {
        this.fedConnection = fedConnection;

        try {
            this.fedConnection.setAutoCommit(false);
        } catch (FedException fedException) {
            System.out.flush();

            Logger.error("An error occurred while setting fed connection auto commit false.", fedException);
            System.err.flush();
        }
    }

    public void run(final String filename) {
        run(filename, false);
    }

    public void run(final String filename, final boolean debug) {
        Logger.logln("**************************************************************************");
        Logger.logln("Executing script file '" + filename + "'...");

        long op = 0;
        long start = System.currentTimeMillis();

        final Scanner scanner = new Scanner(getClass().getClassLoader().getResourceAsStream(filename)).useDelimiter(";");
        //final Scanner scanner = new Scanner(getClass().getResourceAsStream(filename)).useDelimiter(";");
        while (scanner.hasNext()) {

            String statement = scanner.next().trim();
            while (statement.startsWith("/*") || statement.startsWith("--")) {
                if (statement.startsWith("/*")) {
                    final String comment = statement.substring(0, statement.indexOf("*/") + 2);
                    if (debug) {
                        Logger.logln("--> " + comment + " <--");
                    }
                    statement = statement.substring(statement.indexOf("*/") + 2).trim();
                } else {
                    final String comment = statement.substring(0, statement.indexOf("\n") - 1);
                    if (debug) {
                        Logger.logln("--> " + comment + " <--");
                    }
                    statement = statement.substring(statement.indexOf("\n") + 1).trim();
                }
            }

            if (!"".equals(statement)) {
                if (!"SET ECHO ON".equals(statement.toUpperCase()) && !statement.toUpperCase().startsWith("ALTER SESSION")) {
                    if (debug) {
                        Logger.logln("Executing \"" + statement + "\"...\n");
                        System.out.flush();
                    }
                    if (statement.toUpperCase().equals("COMMIT")) {
                        try {
                            fedConnection.commit();
                            if (debug) {
                                Logger.logln("Transaction commit");
                            }
                        } catch (final FedException fedException) {
                            Logger.error("An error occurred while commit fed connection.", fedException);
                            System.out.flush();
                        }
                    } else {
                        if (statement.toUpperCase().equals("ROLLBACK")) {
                            try {
                                fedConnection.rollback();
                                if (debug) {
                                    Logger.logln("Transaction rollback");
                                }
                            } catch (final FedException fedException) {
                                Logger.error("An error occurred while rollback fed connection.", fedException);
                                System.out.flush();
                            }
                        } else {
                            if (statement.toUpperCase().startsWith("SELECT")) {
                                // SELECT
                                try {
                                    final FedStatement fedStatement = fedConnection.getStatement();
                                    final FedResultSet fedResultSet = fedStatement.executeQuery(statement);

                                    op++;

                                    if (debug) {
                                        for (int i = 1; i <= fedResultSet.getColumnCount(); i++) {
                                            String columnName = fedResultSet.getColumnName(i);
                                            if (i == 1) {
                                                Logger.log("\n");
                                            }
                                            Logger.log(String.format("%-15s", columnName));
                                        }

                                        Logger.log("\n");
                                        for (int i = 1; i <= fedResultSet.getColumnCount(); i++) {
                                            Logger.log("-------------- ");
                                        }

                                        while (fedResultSet.next()) {
                                            for (int i = 1; i <= fedResultSet.getColumnCount(); i++) {
                                                String value = fedResultSet.getString(i);
                                                if (i == 1) {
                                                    Logger.log("\n");
                                                }
                                                Logger.log(String.format("%-15s", value));
                                            }
                                            Logger.log("\n");
                                        }
                                        Logger.log("\n");
                                    }
                                    fedStatement.close();
                                } catch (final FedException fedException) {
                                    Logger.error("An error occurred while executing select fed statement.", fedException);
                                    System.out.flush();
                                }
                            } else {
                                // UPDATE, INSERT, DELETE
                                try {
                                    final FedStatement fedStatement = fedConnection.getStatement();
                                    final int count = fedStatement.executeUpdate(statement);

                                    op++;

                                    if (statement.toUpperCase().startsWith("UPDATE")) {
                                        if (debug) {
                                            Logger.logln(count + " rows updated\n");
                                        }
                                    } else {
                                        if (statement.toUpperCase().startsWith("INSERT")) {
                                            if (debug) {
                                                Logger.logln(count + " rows inserted\n");
                                            }
                                        } else {
                                            if (statement.toUpperCase().startsWith("DELETE")) {
                                                if (debug) {
                                                    Logger.logln(count + " rows deleted\n");
                                                }
                                            } else {
                                                if (debug) {
                                                    Logger.logln(count + " OK");
                                                }
                                            }
                                        }
                                    }

                                    fedStatement.close();
                                } catch (final FedException fedException) {
                                    Logger.error("An error occurred while executing update fed statement.", fedException);
                                    System.out.flush();
                                }
                            }
                        }
                    }
                }
            }
        }

        final long end = System.currentTimeMillis();
        final long delta = end - start;

        Logger.logln("File '" + filename + "', " + op + " operations, " + delta + " milliseconds");
        Logger.logln("**************************************************************************\n");
    }
}
