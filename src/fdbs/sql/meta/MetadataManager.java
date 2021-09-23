package fdbs.sql.meta;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.ast.attribute.TableAttribute;
import fdbs.sql.parser.ast.clause.HorizontalClause;
import fdbs.sql.parser.ast.clause.VerticalClause;
import fdbs.sql.parser.ast.clause.boundary.IntegerBoundary;
import fdbs.sql.parser.ast.clause.boundary.StringBoundary;
import fdbs.sql.parser.ast.constraint.CheckConstraint;
import fdbs.sql.parser.ast.constraint.ForeignKeyConstraint;
import fdbs.sql.parser.ast.constraint.PrimaryKeyConstraint;
import fdbs.sql.parser.ast.constraint.UniqueConstraint;
import fdbs.sql.parser.ast.expression.BetweenExpression;
import fdbs.sql.parser.ast.expression.BinaryExpression;
import fdbs.sql.parser.ast.expression.UnaryExpression;
import fdbs.sql.parser.ast.identifier.AttributeIdentifier;
import fdbs.sql.parser.ast.literal.StringLiteral;
import fdbs.sql.parser.ast.statement.CreateTableStatement;
import fdbs.sql.parser.ast.statement.DropTableStatement;
import fdbs.util.logger.Logger;
import fdbs.util.statement.StatementUtil;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MetadataManager implements MetadataManagerInterface {

    private static MetadataManager instance = null;
    private ArrayList<Table> tables = new ArrayList<Table>();
    private HashMap<String, Table> tablesByName = new HashMap();
    private FedConnection fedConnection;
    private Connection connectionDb1;
    private int tableId = 0;
    private String tableIdentifier = "";
    private HashMap<String, String> dataTypeMap = new HashMap<>();
    private HashMap<String, String> dataDefaultMap = new HashMap<>();
    private HorizontalClause horizontalClause;
    private VerticalClause verticalClause;

    private MetadataManager(FedConnection connection) throws FedException {
        this.fedConnection = connection;
        connectionDb1 = connection.getConnections().get(1);
        this.checkAndRefreshTables();
        this.refreshDataSource();
    }

    /**
     * @param connection The FedConnection for the Metadata.
     * @return returns thread safe instance of the singleton PropertyManager
     */
    public static synchronized MetadataManager getInstance(FedConnection connection) throws FedException {
        if (instance == null) {
            instance = new MetadataManager(connection);
        }

        return instance;
    }

    /**
     * @param connection The FedConnection for the Metadata.
     * @return returns thread safe instance of the singleton PropertyManager
     */
    public static synchronized MetadataManager getInstance(FedConnection connection, boolean forceNew) throws FedException {
        if (forceNew || instance == null) {
            instance = new MetadataManager(connection);
        }

        return instance;
    }

    // <editor-fold defaultstate="collapsed" desc="attributeGetters" >
    /**
     * Methode zum Holen der Hashmap der Namen der Tables zu den Tables.
     *
     * @return die Hashmap.
     */
    public HashMap<String, Table> getTablesByName() {
        return tablesByName;
    }

    /**
     * get Horizontal Partitions for Table
     *
     * @param tableName The Name of the Table
     * @return List of all Horizontal Partitions by tableName
     */
    @Override
    public ArrayList<HorizontalPartition> getHorizontalPartition(String tableName) {
        Table table = getTable(tableName);
        if (table == null) {
            return new ArrayList<>();
        } else {
            return table.getHorizontalPartitions();
        }
    }

    /**
     * Get verticalPosition by tableName
     *
     * @param tableName The Name of the Table
     * @return Vertical Columns as Hashmap - error in here?
     */
    @Override
    public HashMap<String, Column> getVerticalPartition(String tableName) {
        Table table = getTable(tableName);
        if (table == null) {
            return new HashMap<>();
        } else {
            return table.getColumnsByName();
        }
    }

    /**
     * Get Column by tableName and columnName
     *
     * @param tableName The Name of the Table
     * @param columnName The Name of the Column
     * @return Column type for tableName as String
     */
    @Override
    public String getColumnType(String tableName, String columnName) {
        Column column = getColumn(tableName, columnName);
        if (column == null) {
            return null;
        } else {
            return column.getType();
        }
    }

    /**
     * Get Column type by tableName and columnName
     *
     * @param tableName The Name of the Table
     * @param columnName The Name of the Column
     * @return Full Column for tableName as Column class
     */
    @Override
    public Column getColumn(String tableName, String columnName) {
        Table table = getTable(tableName);
        if (table == null) {
            return null;
        } else {
            return table.getColumn(columnName);
        }
    }

    /**
     * Get Columns of Metadata tableName
     *
     * @param tableName The Name of the Table
     * @return All Columns for tableName as Arraylist
     */
    @Override
    public ArrayList<Column> getColumns(String tableName) {
        Table table = getTable(tableName);
        if (table == null) {
            return new ArrayList<>();
        } else {
            return table.getColumns();
        }
    }

    /**
     * Get Columns of Metadata by tableName and Database
     *
     * @param tableName The Name of the Table
     * @param db database Id
     * @return All Columns for tableName by database ID as Arraylist
     */
    @Override
    public HashMap<String, Column> getColumnsByDatabase(String tableName, int db) {
        Table table = getTable(tableName);
        if (table == null || db < 1 || db > 3) {
            return new HashMap<>();
        } else {
            return table.getColumnsByDatabase(db);
        }
    }

    /**
     * Get all Metadata columns
     *
     * @return Arraylist of all tables
     */
    @Override
    public ArrayList<Column> getAllColumns() {
        ArrayList<Column> allColumns = new ArrayList();

        for (Table table : tables) {
            allColumns.addAll(table.getColumns());
        }

        return allColumns;
    }

    /**
     * Get all Metadata Tables
     *
     * @return Arraylist of all tables
     */
    @Override
    public ArrayList<Table> getAllTables() {
        return tables;
    }

    /**
     * Get Metadata Table by String
     *
     * @param tableName The Name of the Table
     *
     * @return ArrayList table else null
     */
    @Override
    public Table getTable(String tableName) {
        return this.tablesByName.get(tableName);
    }

    /**
     * Methode zum Holen des Tables mit der tableId.
     *
     * @param tableId Die Id des Tables
     * @return Die Tabellle
     */
    public Table getTable(int tableId) {
        for (Table table : tables) {
            if (table.getId() == tableId) {
                return table;
            }
        }

        return null;
    }

    /**
     * Get all Constraints
     *
     * @return ArrayList of all Contraints for all tables
     */
    @Override
    public ArrayList<Constraint> getAllConstraints() {
        ArrayList<Constraint> allConstraints = new ArrayList();

        for (Table table : tables) {
            allConstraints.addAll(table.getConstraints());
        }

        return allConstraints;
    }

    /**
     * Get all Constraints by tableName
     *
     * @param tableName The Name of the Table
     * @return ArrayList of all Contraints for all tables as Arraylist
     */
    @Override
    public ArrayList<Constraint> getConstraintsByTable(String tableName) {
        Table table = getTable(tableName);
        if (table == null) {
            return new ArrayList<>();
        } else {
            return table.getConstraints();
        }
    }

    /**
     * Get Constraint by tableName, database id and constraintName
     *
     * @param tableName The Name of the Table
     * @param db The id of the Database
     * @param constraintName The constraint Name which should be returned
     * @return full Constraint class values of tableName, database and
     * constraintName
     */
    @Override
    public Constraint getConstraint(String tableName, int db, String constraintName) {
        Table table = getTable(tableName);
        if (table == null) {
            return null;
        } else {
            return table.getConstraint(db, constraintName);
        }
    }

    /**
     * Get List of all Constraints by tableName, database id
     *
     * @param tableName The Name of the Table
     * @param db The id of the Database
     * @return List of all Constraint classes by tableName and database id
     */
    @Override
    public ArrayList<Constraint> getConstraints(String tableName, int db) {
        Table table = getTable(tableName);
        if (table == null) {
            return null;
        } else {
            return table.getConstraintsByDatabase(db);
        }
    }

    /**
     * Get Partition Type by tableName
     *
     * @param tableName The Name of the Table
     * @return PartitionType class by tableName
     */
    @Override
    public PartitionType getPartitionType(String tableName) {
        Table table = getTable(tableName);
        if (table == null) {
            return null;
        } else {
            return table.getPartitionType();
        }
    }
    //</editor-fold>

    // <editor-fold defaultstate="collapsed" desc="deleteMetaData" >
    /**
     * Delete all rows in the Metadata-Tables. Useful to reset data for testing
     * purposes.
     *
     * @throws fdbs.sql.FedException
     */
    @Override
    public void deleteMetadata() throws FedException {
        List<SQLException> occuredExceptions = new ArrayList<>();

        String[] executingStatements = new String[]{
            "DELETE Meta_Table_Constraints",
            "DELETE Meta_Table_Constraints_Between",
            "DELETE Meta_Table_Constraints_Binary",
            "DELETE Meta_Horizontal",
            "DELETE Meta_Columns",
            "DELETE Meta_Table_Partition"
        };

        for (String stmt : executingStatements) {
            try {
                this.executeDDL(stmt).close();
            } catch (SQLException ex) {
                occuredExceptions.add(ex);
            }
        }

        if (occuredExceptions.size() > 0) {
            FedException fedException = new FedException(String.format("%s: An error occurred while deleting all metadata tables.", this.getClass().getSimpleName()));

            occuredExceptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            throw fedException;
        }
    }
    //</editor-fold>

    // <editor-fold defaultstate="collapsed" desc="handleQuery" >
    /**
     * Handles Create or Delete Query and updates Metadata Table.
     *
     * @param tree The Create or Delete Statement AST.
     * @throws fdbs.sql.FedException
     */
    @Override
    public void handleQuery(AST tree) throws FedException {
        ASTNode statement = tree.getRoot();

        // Default Values setzen
        this.tableId = 0;
        this.tableIdentifier = "";
        this.dataTypeMap = new HashMap<>();
        this.dataDefaultMap = new HashMap<>();
        this.horizontalClause = null;
        this.verticalClause = null;

        if (statement instanceof CreateTableStatement) {
            handleCreateQuery(statement);
            this.refreshDataSource();
        } else if (statement instanceof DropTableStatement) {
            handleDropQuery(statement);
            this.refreshDataSource();
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="deleteTables" >
    /**
     * Methode zum Löschen der MetaDatenTabellen.
     *
     * @throws fdbs.sql.FedException
     */
    public void deleteTables() throws FedException {
        List<Exception> occuredExceptions = new ArrayList<>();

        try (InputStream input = this.getClass().getClassLoader().getResourceAsStream("fdbs/sql/meta/dropMetadataTable.sql")) {
            String[] statements = StatementUtil.getStatementsFromInputStream(input);

            // Die Statements durchlaufen und ausführen
            for (String stm : statements) {
                try {
                    try (Statement statement = connectionDb1.createStatement()) {
                        Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", stm));
                        statement.executeQuery(stm).close();
                    }
                } catch (SQLException ex) {
                    occuredExceptions.add(ex);
                }
            }

            input.close();
        } catch (IOException ex) {
            Logger.error(String.format("%s: An error occurred while reading federated middleware properties.", this.getClass().getSimpleName()), ex);
        }

        clearLocalData();

        if (occuredExceptions.size() > 0) {
            FedException fedException = new FedException(String.format("%s: An error occurred while deleting all metadata tables.", this.getClass().getSimpleName()));

            occuredExceptions.forEach((exception) -> {
                fedException.addSuppressed(exception);
            });

            throw fedException;
        }
    }
    //</editor-fold>

    // <editor-fold defaultstate="collapsed" desc="checkAndRefreshTables" >
    /**
     * Methode zum Überprüfen, ob die Tabellen vorhanden sind. Erstellt diese
     * sonst neu.
     *
     * @throws fdbs.sql.FedException Werfen der FedException.
     */
    public void checkAndRefreshTables() throws FedException {
        int metadataTableCounter = 0;

        // Die Tabellen mit den Namen "META_" am Anfang holen und zählen
        try {
            DatabaseMetaData md = connectionDb1.getMetaData();
            try (ResultSet rs = md.getTables(null, null, "META_%", null)) {
                while (rs.next()) {
                    metadataTableCounter++;
                }
            }
        } catch (SQLException ex) {
            throw new FedException(String.format("%s: An error occurred while deleting all metadata tables.", this.getClass().getSimpleName()), ex);
        }

        // Falls die sechs nicht MetaDatenTabellen nicht vorhanden sind, müssen diese erstellt werden
        if (metadataTableCounter != 6) {
            try (InputStream input = this.getClass().getClassLoader().getResourceAsStream("fdbs/sql/meta/createMetadataTable.sql")) {
                String[] statements = StatementUtil.getStatementsFromInputStream(input);

                // Die Statements durchlaufen und ausführen
                for (String stm : statements) {
                    Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", stm));
                    try (Statement statement = connectionDb1.createStatement()) {
                        statement.executeQuery(stm).close();
                    }
                }

                input.close();
            } catch (SQLException | IOException ex) {
                throw new FedException(String.format("%s: An error occurred while checking and refreshing metadata tables.", this.getClass().getSimpleName()), ex);
            }
        }

        refreshDataSource();
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="refreshDataSource" >
    /**
     * Methode to Refresh MetaDataDataSource with existing Connection.
     */
    private void refreshDataSource() throws FedException {
        try {
            clearLocalData();

            String constraintsQuery = ""
                    + "SELECT c.*, be.min_type, be.min_value, be.max_type, be.max_value, bi.operator, bi.binary_type, bi.left_column_name, bi.left_column_type, bi.right_literal_value, bi.right_literal_type "
                    + "FROM Meta_Table_Constraints c "
                    + "LEFT JOIN Meta_Table_Constraints_Between be ON c.between_constraint_id = be.between_constraint_id "
                    + "LEFT JOIN Meta_Table_Constraints_Binary bi ON c.binary_constraint_id = bi.binary_constraint_id "
                    + "ORDER BY table_id, database_id";

            String stmtTables = "SELECT * FROM Meta_Table_Partition ORDER BY table_id";
            String stmtColumns = "SELECT * FROM Meta_Columns ORDER BY table_id, column_id";
            String stmtHorizontals = "SELECT * FROM Meta_Horizontal ORDER BY table_id, horizontal_id";

            Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", stmtTables));
            Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", stmtColumns));
            Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", constraintsQuery));
            Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", stmtHorizontals));

            try (Statement statementTable = connectionDb1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    Statement statementColumn = connectionDb1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    Statement statementConstraint = connectionDb1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    Statement statementHorizontal = connectionDb1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    ResultSet rsTables = statementTable.executeQuery(stmtTables);
                    ResultSet rsColumns = statementColumn.executeQuery(stmtColumns);
                    ResultSet rsConstraints = statementConstraint.executeQuery(constraintsQuery);
                    ResultSet rsHorizontals = statementHorizontal.executeQuery(stmtHorizontals);) {

                while (rsTables.next()) {
                    int table = rsTables.getInt("table_id");
                    String tableName = rsTables.getString("table_name");

                    ArrayList<Column> columns = new ArrayList();
                    while (rsColumns.next()) {
                        if (rsColumns.getInt("table_id") == table) {
                            Column column = new Column(rsColumns);
                            columns.add(column);
                        } else {
                            rsColumns.previous();
                            break;
                        }
                    }

                    ArrayList<Constraint> constraints = new ArrayList();
                    while (rsConstraints.next()) {
                        if (rsConstraints.getInt("table_id") == table) {
                            if (rsConstraints.getInt("between_constraint_id") == 0 && rsConstraints.getInt("binary_constraint_id") == 0) {
                                Constraint constraint = new Constraint(rsConstraints);
                                constraints.add(constraint);
                            } else {
                                if (rsConstraints.getInt("between_constraint_id") != 0) {
                                    BetweenConstraint constraint = new BetweenConstraint(rsConstraints);
                                    constraints.add(constraint);
                                } else {
                                    BinaryConstraint constraint = new BinaryConstraint(rsConstraints);
                                    constraints.add(constraint);
                                }
                            }
                        } else {
                            rsConstraints.previous();
                            break;
                        }
                    }

                    ArrayList<HorizontalPartition> horizontalPartitions = new ArrayList();
                    while (rsHorizontals.next()) {
                        if (rsHorizontals.getInt("table_id") == table) {
                            HorizontalPartition horizontal = new HorizontalPartition(rsHorizontals);
                            horizontalPartitions.add(horizontal);
                        } else {
                            rsHorizontals.previous();
                            break;
                        }
                    }

                    Table completeTable = new Table(rsTables, columns, constraints, horizontalPartitions);
                    tables.add(completeTable);
                    tablesByName.put(tableName, completeTable);

                }
            }
        } catch (SQLException ex) {
            throw new FedException(String.format("%s: An error occurred while refreshing data source.", this.getClass().getSimpleName()), ex);
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="handleCreateQuery" >
    /**
     * Methode zum Handlen der Create-Query
     *
     * @param statement Das AST-Statement.
     */
    private void handleCreateQuery(ASTNode statement) throws FedException {
        try {
            List<fdbs.sql.parser.ast.constraint.Constraint> constraintList;
            List<TableAttribute> attributeList;
            CreateTableStatement createTableStatement = (CreateTableStatement) statement;
            tableIdentifier = createTableStatement.getTableIdentifier().getIdentifier();
            attributeList = createTableStatement.getAttributes();

            //iterate all columns and use a hash map to map their name with their data type definition.
            //e.g.: Key: FHC
            //      Value: VARCHAR (3)
            for (TableAttribute attribute : attributeList) {
                String dataTypeDescriptor = attribute.getType().getTypeDescriptor();

                dataTypeMap.put(attribute.getIdentifierNode().getIdentifier(), dataTypeDescriptor);

                // Den Default Value abspeichern
                if (attribute.getDefaultValue() != null) {
                    dataDefaultMap.put(attribute.getIdentifierNode().getIdentifier(), attribute.getDefaultValue().getIdentifier());
                }
            }

            horizontalClause = createTableStatement.getHorizontalClause();
            this.processHorizontal();

            verticalClause = createTableStatement.getVerticalClause();
            if (verticalClause != null) {
                this.processVertical();
            } else {
                this.processColumns(attributeList);
            }

            constraintList = createTableStatement.getConstraints();
            for (fdbs.sql.parser.ast.constraint.Constraint constraint : constraintList) {
                if (constraint instanceof CheckConstraint) {
                    CheckConstraint checkConstraint = (CheckConstraint) constraint;
                    switch (checkConstraint.getCheckExpression().getOperatorName()) {
                        case "EQ":
                        case "NEQ":
                        case "LEQ":
                        case "GEQ":
                        case "LT":
                        case "GT":
                            this.processCheckBinaryConstraint(checkConstraint);
                            break;
                        case "BETWEEN":
                            this.processCheckBetweenConstraint(checkConstraint);
                            break;
                        case "ISNOTNULL":
                            this.processCheckNotNullConstraint(checkConstraint);
                            break;
                        default:
                            break;
                    }
                } else if (constraint instanceof ForeignKeyConstraint) {
                    this.processForeignKeyConstraint(constraint);
                } else if (constraint instanceof PrimaryKeyConstraint) {
                    this.processPrimaryKeyConstraint(constraint);
                } else if (constraint instanceof UniqueConstraint) {
                    this.processUniqueConstraint(constraint);
                }
            }
        } catch (SQLException ex) {
            throw new FedException(String.format("%s: An error occurred while handling a create table statement.", this.getClass().getSimpleName()), ex);
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="handleDropQuery" >
    /**
     * Methode zum Handlen der Drop-Query.
     *
     * @param statement Das Statement des ASTs.
     */
    private void handleDropQuery(ASTNode statement) throws FedException {
        DropTableStatement drop = (DropTableStatement) statement;
        String identifier = drop.getTableIdentifier().getIdentifier();

        // Alle Tabellen durchlaufen und die richtigen Tabelle ermitteln.
        tableId = this.getTable(identifier).getId();

        // Falls keine Tabelle gefunden wurde, eine Exception werfen.
        // Sonst wird die Tabelle aus den MetaDaten gelöscht.
        if (tableId == 0) {
            throw new FedException(String.format("%s: The table with the name \"%s\" does not exists.", this.getClass().getSimpleName(), identifier));
        } else {
            List<SQLException> occuredExceptions = new ArrayList<>();

            String[] executingStatements = new String[]{
                String.format("DELETE FROM ( SELECT b.* from Meta_Table_Constraints_Between b INNER JOIN Meta_Table_Constraints c ON c.between_constraint_id = b.between_constraint_id WHERE (c.table_id = %d))", tableId),
                String.format("DELETE FROM ( SELECT b.* from Meta_Table_Constraints_Binary b INNER JOIN Meta_Table_Constraints c ON c.binary_constraint_id = b.binary_constraint_id WHERE (c.table_id = %d))", tableId),
                String.format("DELETE FROM Meta_Horizontal WHERE (table_id = %d)", tableId),
                String.format("DELETE FROM Meta_Table_Constraints WHERE (table_id = %d)", tableId),
                String.format("DELETE FROM Meta_Columns WHERE (table_id = %d)", tableId),
                String.format("DELETE FROM Meta_Table_Partition WHERE (table_id = %d)", tableId)};

            for (String stmt : executingStatements) {
                try {
                    this.executeDDL(stmt).close();
                } catch (SQLException ex) {
                    occuredExceptions.add(ex);
                }
            }

            if (occuredExceptions.size() > 0) {
                FedException fedException = new FedException(String.format("%s: An error occurred while handling a drop table statement.", this.getClass().getSimpleName()));

                occuredExceptions.forEach((exception) -> {
                    fedException.addSuppressed(exception);
                });

                throw fedException;
            }
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="processColumnsWithoutPartition" >
    /**
     * Methode zum Erstellen der Tabelle, falls keine Partition vorliegt und
     * erstellen der Spalten.
     *
     * @param attributeList Die Liste der Tabellenattribute
     * @throws FedException Weiterwerfen der Exception.
     * @throws SQLException Weiterwerfen der Exception.
     */
    private void processColumns(List<TableAttribute> attributeList) throws FedException, SQLException {
        String dbId = "1";

        if (verticalClause == null && horizontalClause == null) {
            String sql = String.format("INSERT INTO Meta_Table_Partition (table_id,table_name,partition_type) VALUES (mtp_seq.nextval,'%s','%s')", tableIdentifier, 'N');

            this.executeDDL(sql).close();
            try (Statement sqlStatement = this.executeDDL("SELECT mtp_seq.currval FROM dual");
                    ResultSet rs = sqlStatement.getResultSet()) {

                if (rs.next()) {
                    tableId = rs.getInt(1);
                }
            }
        } else {
            // Sonst dbId null reinschreiben, da es auf allen Datenbanken vorliegt
            dbId = "null";
        }

        for (TableAttribute tableAttribute : attributeList) {
            String identifier = tableAttribute.getIdentifierNode().getIdentifier();
            String defaultValue = dataDefaultMap.get(identifier);
            if (defaultValue != null) {
                String sql = String.format("INSERT INTO Meta_Columns (table_id,column_name,column_type,column_default, database_id ) "
                        + "VALUES (%d, '%s', '%s', '%s' , %s)", tableId, identifier, dataTypeMap.get(identifier), defaultValue, dbId);
                this.executeDDL(sql).close();
            } else {
                String sql = String.format("INSERT INTO Meta_Columns (table_id,column_name,column_type,column_default, database_id ) "
                        + "VALUES (%d,'%s','%s', null , %s)", tableId, identifier, dataTypeMap.get(identifier), dbId);
                this.executeDDL(sql).close();
            }
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="processHorizontal" >
    /**
     * Methode zum Verarbeiten der Horizontals.
     *
     * @throws FedException Weiterwerfen der Exception.
     * @throws SQLException Weiterwerfen der Exception.
     */
    private void processHorizontal() throws FedException, SQLException {
        if (horizontalClause != null) {
            String sql = String.format("INSERT INTO Meta_Table_Partition (table_id,table_name,partition_type) VALUES (mtp_seq.nextval,'%s','%s')", tableIdentifier, 'H');

            String type = dataTypeMap.get(horizontalClause.getAttributeIdentifier().getIdentifier());

            this.executeDDL(sql).close();
            try (Statement sqlStatement = this.executeDDL("SELECT mtp_seq.currval FROM dual");
                    ResultSet rs = sqlStatement.getResultSet()) {
                if (rs.next()) {
                    tableId = rs.getInt(1);
                }
            }

            if (horizontalClause.getSecondBoundary() instanceof IntegerBoundary) {
                IntegerBoundary highBoundary = (IntegerBoundary) horizontalClause.getSecondBoundary();
                int highIntegerBoundary = Integer.parseInt(highBoundary.getLiteral().getIdentifier());

                if (horizontalClause.getFirstBoundary() != null) {
                    IntegerBoundary lowBoundary = (IntegerBoundary) horizontalClause.getFirstBoundary();
                    int lowIntegerBoundary = Integer.parseInt(lowBoundary.getLiteral().getIdentifier());

                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', null, '%s', '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, lowIntegerBoundary, lowIntegerBoundary, 1);
                    this.executeDDL(sql).close();

                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', '%s','%s', '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, lowIntegerBoundary + 1, highIntegerBoundary, highIntegerBoundary, 2);
                    this.executeDDL(sql).close();
                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', '%s', null, '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, highIntegerBoundary + 1, highIntegerBoundary + 1, 3);
                    this.executeDDL(sql).close();
                } else {
                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', null,'%s', '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, highIntegerBoundary, highIntegerBoundary, 1);
                    this.executeDDL(sql).close();

                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', '%s', null, '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, highIntegerBoundary + 1, highIntegerBoundary + 1, 2);
                    this.executeDDL(sql).close();
                }
            } else {
                StringBoundary highBoundary = (StringBoundary) horizontalClause.getSecondBoundary();

                //high is inclusive max for db2
                //high is exclusive min for db3
                String highLiteral = highBoundary.getLiteral().getIdentifier();

                if (horizontalClause.getFirstBoundary() != null) {
                    StringBoundary lowBoundary = (StringBoundary) horizontalClause.getFirstBoundary();

                    //low is max for DB1, low is exclusive min for DB2
                    String lowLiteral = lowBoundary.getLiteral().getIdentifier();

                    //naiver Ansatz
                    int t = lowLiteral.charAt((lowLiteral.length() - 1));

                    String mediumLow = (lowLiteral.substring(0, lowLiteral.length() - 1)) + String.valueOf(((char) (t + 1)));
                    t = highLiteral.charAt(highLiteral.length() - 1);

                    String upperBoundaryString = (highLiteral.substring(0, highLiteral.length() - 1)) + String.valueOf(((char) (t + 1)));

                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', null,'%s', '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, lowLiteral, lowLiteral, 1);
                    this.executeDDL(sql).close();

                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', '%s','%s', '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, lowLiteral, highLiteral, mediumLow, 2);
                    this.executeDDL(sql).close();

                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', '%s', null, '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, highLiteral, upperBoundaryString, 3);
                    this.executeDDL(sql).close();
                } else {
                    int t = highLiteral.charAt(highLiteral.length() - 1);

                    String upperBoundaryString = (highLiteral.substring(0, highLiteral.length() - 1)) + String.valueOf(((char) (t + 1)));

                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', null, '%s', '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, highLiteral, highLiteral, 1);
                    this.executeDDL(sql).close();

                    sql = String.format("INSERT INTO Meta_Horizontal "
                            + "(table_id, attribute_name,attribute_type,value_lowest,value_highest,value_default,database_id) "
                            + "VALUES (%d, '%s', '%s', '%s', null, '%s', %d)", tableId, horizontalClause.getAttributeIdentifier().getIdentifier(), type, highLiteral, upperBoundaryString, 2);
                    this.executeDDL(sql).close();
                }
            }
        } else {
            Logger.debugln(String.format("%s: No horizontal clause found.", this.getClass().getSimpleName()));
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="processVertical" >
    /**
     * Methode zum Erstellen der vertikalen Columns.
     *
     * @throws FedException Weiterwerfen der Exception.
     * @throws SQLException Weiterwerfen der Exception.
     */
    private void processVertical() throws FedException, SQLException {
        String sql = String.format("INSERT INTO Meta_Table_Partition (table_id,table_name,partition_type) VALUES (mtp_seq.nextval,'%s','%s')", tableIdentifier, 'V');
        this.executeDDL(sql).close();

        try (Statement sqlStatement = this.executeDDL("SELECT mtp_seq.currval FROM dual");
                ResultSet rs = sqlStatement.getResultSet()) {

            if (rs.next()) {
                tableId = rs.getInt(1);
            }
        }

        for (AttributeIdentifier attributeIdentifier : verticalClause.getAttributesForDB1()) {
            String defaultValue = dataDefaultMap.get(attributeIdentifier.getIdentifier());

            if (defaultValue != null) {
                sql = String.format("INSERT INTO Meta_Columns (table_id,column_name,column_type,column_default,database_id ) "
                        + "VALUES (%d, '%s','%s', '%s', %d)", tableId, attributeIdentifier.getIdentifier(), dataTypeMap.get(attributeIdentifier.getIdentifier()), defaultValue, 1);
            } else {
                sql = String.format("INSERT INTO Meta_Columns (table_id,column_name,column_type,column_default,database_id ) "
                        + "VALUES (%d, '%s','%s', null, %d)", tableId, attributeIdentifier.getIdentifier(), dataTypeMap.get(attributeIdentifier.getIdentifier()), 1);
            }

            this.executeDDL(sql).close();
        }

        for (AttributeIdentifier attributeIdentifier : verticalClause.getAttributesForDB2()) {
            String defaultValue = dataDefaultMap.get(attributeIdentifier.getIdentifier());
            if (defaultValue != null) {
                sql = String.format("INSERT INTO Meta_Columns (table_id,column_name,column_type,column_default, database_id ) "
                        + "VALUES (%d,'%s','%s', '%s',%d)", tableId, attributeIdentifier.getIdentifier(), dataTypeMap.get(attributeIdentifier.getIdentifier()), defaultValue, 2);
            } else {
                sql = String.format("INSERT INTO Meta_Columns (table_id,column_name,column_type,column_default, database_id ) "
                        + "VALUES (%d,'%s','%s', null,%d)", tableId, attributeIdentifier.getIdentifier(), dataTypeMap.get(attributeIdentifier.getIdentifier()), 2);
            }

            this.executeDDL(sql).close();
        }

        for (AttributeIdentifier attributeIdentifier : verticalClause.getAttributesForDB3()) {
            String defaultValue = dataDefaultMap.get(attributeIdentifier.getIdentifier());

            if (defaultValue != null) {
                sql = String.format("INSERT INTO Meta_Columns (table_id,column_name,column_type,column_default,database_id ) "
                        + "VALUES (%d, '%s','%s', '%s', %d)", tableId, attributeIdentifier.getIdentifier(), dataTypeMap.get(attributeIdentifier.getIdentifier()), defaultValue, 3);
            } else {
                sql = String.format("INSERT INTO Meta_Columns (table_id,column_name,column_type,column_default,database_id ) "
                        + "VALUES (%d, '%s', '%s', null, %d)", tableId, attributeIdentifier.getIdentifier(), dataTypeMap.get(attributeIdentifier.getIdentifier()), 3);
            }

            this.executeDDL(sql).close();
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="processConstraints" >
    // <editor-fold defaultstate="collapsed" desc="PrimaryKey" >
    /**
     * Methode zum Verarbeiten eines PrimaryKey-Constraints.
     *
     * @param constraint Das constraint.
     */
    private void processPrimaryKeyConstraint(fdbs.sql.parser.ast.constraint.Constraint constraint) throws FedException, SQLException {
        PrimaryKeyConstraint primaryKeyConstraint = (PrimaryKeyConstraint) constraint;
        int dbId = 0;

        if (horizontalClause == null && verticalClause == null) {
            // Set dbId on 1, because no partition is avaiable
            dbId = 1;

            String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                    + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, dbId, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
            this.executeDDL(sql).close();
        } else {
            if (horizontalClause != null) {
                // Falls der PrimaryKey durch die horizontale Aufteilung fixiert ist, muss dieser durch die Teiltabellen überprüft werden
                if (horizontalClause.getAttributeIdentifier().getIdentifier().equals(primaryKeyConstraint.getAttributeIdentifier().getIdentifier())) {
                    // Falls er drei oder zwei Grenzen hat
                    if (horizontalClause.getFirstBoundary() != null) {
                        String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 1, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 2, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 3, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();
                    } else {
                        String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 1, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 2, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();
                    }
                } else {
                    // Primary Key über die Middleware überprüfen lassen
                    String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                            + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, dbId, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                    this.executeDDL(sql).close();
                }
            } else {
                // Wenn vertikal partitioniert wird für jede Teiltabelle einfügen
                if (verticalClause != null) {
                    if (!verticalClause.getAttributesForDB3().isEmpty()) {
                        String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 1, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 2, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 3, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();
                    } else {
                        String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 1, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 2, primaryKeyConstraint.getConstraintIdentifier().getIdentifier(), 'P', primaryKeyConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();
                    }
                }
            }
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="ForeignKey" >
    /**
     * Methode zum Verarbeiten eines foreignKeyConstraints.
     *
     * @param constraint Das constraint.
     * @throws FedException Weiterwerfen der FedException.
     * @throws SQLException Weiterwerfen der SQLException.
     */
    private void processForeignKeyConstraint(fdbs.sql.parser.ast.constraint.Constraint constraint) throws FedException, SQLException {
        ForeignKeyConstraint foreignKeyConstraint = (ForeignKeyConstraint) constraint;
        int dbId = 0;
        boolean isInserted = false;
        Table referencedTable = this.getTable(foreignKeyConstraint.getReferencedTable().getIdentifier());
        PartitionType referencedPartitionType = referencedTable.getPartitionType();

        // checks if dbId needs to be changed
        if (verticalClause == null && horizontalClause == null) {
            // Wenn beide auf dbId 1 liegen, also PartitionType der referenzierten None oder Vertical, da immer auf Schlüssel referenziert wird
            // und dieser bei vertikaler Partionierung überall vorliegt
            if (referencedPartitionType == PartitionType.None || (referencedPartitionType == PartitionType.Vertical)) {
                dbId = 1;
            }
        } else {
            if (verticalClause != null) {
                int columnDbId;
                String columnStmt = "SELECT database_id FROM Meta_Columns WHERE column_name = '" + foreignKeyConstraint.getAttributeIdentifier().getIdentifier() + "' AND TABLE_ID = " + tableId;
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", columnStmt));
                try (Statement columnStatement = connectionDb1.createStatement();
                        ResultSet rs = columnStatement.executeQuery(columnStmt);) {

                    columnDbId = 0;
                    if (rs.next()) {
                        columnDbId = rs.getInt("database_id");
                    }
                }

                // Falls diese Tabelle und die referenzierte Vertical
                if (referencedPartitionType == PartitionType.Vertical) {
                    // Falls beide auf der gleichen Db liegen und beide vertikal aufgeteilt sind
                    if (columnDbId == 1 || columnDbId == 2 || (columnDbId == 3 && !referencedTable.getDatabaseRelatedColumns().get(3).isEmpty())) {
                        dbId = columnDbId;
                    } else {
                        dbId = 0;
                    }
                } else {
                    // Falls die referenzierte Tabelle nicht aufgeteilt aber diese und beide auf db1 sind.
                    if (referencedPartitionType == PartitionType.None && columnDbId == 1) {
                        dbId = 1;
                    }
                }
            } else {
                if (horizontalClause != null) {
                    // Diese Tabelle ist horizontal aufgeteilt
                    // Nun prüfen, dass die referenzierte vertikal aufgeteilt ist 
                    if (referencedPartitionType == PartitionType.Vertical) {
                        // Falls die referenzierte Tabelle auf allen drei Tabellen liegt
                        if (!referencedTable.getDatabaseRelatedColumns().get(3).isEmpty()) {

                            // Den foreign key für jede horizontale Tabelle einfügen
                            String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                    + "VALUES (%d,'%d','%s','%s','%s','%s','%s')", tableId, 1, foreignKeyConstraint.getConstraintIdentifier().getIdentifier(), 'F', foreignKeyConstraint.getAttributeIdentifier().getIdentifier(), foreignKeyConstraint.getReferencedTable().getIdentifier(), foreignKeyConstraint.getReferencedAttribute().getIdentifier());
                            this.executeDDL(sql).close();

                            sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                    + "VALUES (%d,'%d','%s','%s','%s','%s','%s')", tableId, 2, foreignKeyConstraint.getConstraintIdentifier().getIdentifier(), 'F', foreignKeyConstraint.getAttributeIdentifier().getIdentifier(), foreignKeyConstraint.getReferencedTable().getIdentifier(), foreignKeyConstraint.getReferencedAttribute().getIdentifier());
                            this.executeDDL(sql).close();

                            if (horizontalClause.getFirstBoundary() != null) {
                                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                        + "VALUES (%d,'%d','%s','%s','%s','%s','%s')", tableId, 3, foreignKeyConstraint.getConstraintIdentifier().getIdentifier(), 'F', foreignKeyConstraint.getAttributeIdentifier().getIdentifier(), foreignKeyConstraint.getReferencedTable().getIdentifier(), foreignKeyConstraint.getReferencedAttribute().getIdentifier());
                                this.executeDDL(sql).close();
                            }

                            isInserted = true;

                        } else {
                            if (horizontalClause.getFirstBoundary() == null && referencedTable.getDatabaseRelatedColumns().get(3).isEmpty()) {
                                String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                        + "VALUES (%d,'%d','%s','%s','%s','%s','%s')", tableId, 1, foreignKeyConstraint.getConstraintIdentifier().getIdentifier(), 'F', foreignKeyConstraint.getAttributeIdentifier().getIdentifier(), foreignKeyConstraint.getReferencedTable().getIdentifier(), foreignKeyConstraint.getReferencedAttribute().getIdentifier());
                                this.executeDDL(sql).close();

                                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                        + "VALUES (%d,'%d','%s','%s','%s','%s','%s')", tableId, 2, foreignKeyConstraint.getConstraintIdentifier().getIdentifier(), 'F', foreignKeyConstraint.getAttributeIdentifier().getIdentifier(), foreignKeyConstraint.getReferencedTable().getIdentifier(), foreignKeyConstraint.getReferencedAttribute().getIdentifier());
                                this.executeDDL(sql).close();

                                isInserted = true;
                            }
                        }
                    }
                }
            }
        }

        // Falls es noch nicht inserted wurde, einfügen
        if (!isInserted) {
            String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                    + "VALUES (%d,'%d','%s','%s','%s','%s','%s')", tableId, dbId, foreignKeyConstraint.getConstraintIdentifier().getIdentifier(), 'F', foreignKeyConstraint.getAttributeIdentifier().getIdentifier(), foreignKeyConstraint.getReferencedTable().getIdentifier(), foreignKeyConstraint.getReferencedAttribute().getIdentifier());
            this.executeDDL(sql).close();
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Unique">
    /**
     * Methode zum Verarbeiten eines Unique-Constraints.
     *
     * @param constraint Das constraint.
     * @throws FedException Weiterwerfen der FedException.
     * @throws SQLException Weiterwerfen der SQLException.
     */
    private void processUniqueConstraint(fdbs.sql.parser.ast.constraint.Constraint constraint) throws FedException, SQLException {
        UniqueConstraint uniqueConstraint = (UniqueConstraint) constraint;
        int dbId = 0;

        if (horizontalClause == null && verticalClause == null) {
            // Set dbId on 1, because no partition is avaiable
            dbId = 1;

            String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                    + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, dbId, uniqueConstraint.getConstraintIdentifier().getIdentifier(), 'U', uniqueConstraint.getAttributeIdentifier().getIdentifier(), null, null);
            this.executeDDL(sql).close();
        } else {
            if (horizontalClause != null) {
                // Falls das Unique durch die horizontale Aufteilung fixiert ist, muss dieser durch die Teiltabellen überprüft werden
                if (horizontalClause.getAttributeIdentifier().getIdentifier().equals(uniqueConstraint.getAttributeIdentifier().getIdentifier())) {
                    if (horizontalClause.getFirstBoundary() != null) {
                        String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 1, uniqueConstraint.getConstraintIdentifier().getIdentifier(), 'U', uniqueConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 2, uniqueConstraint.getConstraintIdentifier().getIdentifier(), 'U', uniqueConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 3, uniqueConstraint.getConstraintIdentifier().getIdentifier(), 'U', uniqueConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();
                    } else {
                        String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 1, uniqueConstraint.getConstraintIdentifier().getIdentifier(), 'U', uniqueConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();

                        sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                                + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, 2, uniqueConstraint.getConstraintIdentifier().getIdentifier(), 'U', uniqueConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                        this.executeDDL(sql).close();
                    }
                } else {

                    // Unique über die Middleware überprüfen lassen
                    String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                            + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, dbId, uniqueConstraint.getConstraintIdentifier().getIdentifier(), 'U', uniqueConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                    this.executeDDL(sql).close();
                }
            } else {
                // Wenn vertikal partitioniert wird für die Teiltabelle, wo das Attribut vorliegt, einfügen
                if (verticalClause != null) {
                    String columnStmt = "SELECT database_id FROM Meta_Columns WHERE column_name = '" + uniqueConstraint.getAttributeIdentifier().getIdentifier() + "' AND TABLE_ID = " + tableId;
                    Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", columnStmt));
                    try (Statement columnStatement = connectionDb1.createStatement();
                            ResultSet rs = columnStatement.executeQuery(columnStmt);) {

                        if (rs.next()) {
                            dbId = rs.getInt("database_id");
                        }
                    }

                    String sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                            + "VALUES (%d, %d,'%s','%s','%s', %s, %s)", tableId, dbId, uniqueConstraint.getConstraintIdentifier().getIdentifier(), 'U', uniqueConstraint.getAttributeIdentifier().getIdentifier(), null, null);
                    this.executeDDL(sql).close();
                }
            }
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="CheckNotNull" >
    /**
     * Methode zum Verarbeiten eines CheckNotNull-Constraints.
     *
     * @param checkConstraint Das constraint.
     * @throws FedException Weiterwerfen der FedException.
     * @throws SQLException Weiterwerfen der SQLException.
     */
    private void processCheckNotNullConstraint(fdbs.sql.parser.ast.constraint.CheckConstraint checkConstraint) throws FedException, SQLException {
        UnaryExpression unaryExpressionNN = (UnaryExpression) checkConstraint.getCheckExpression();
        String sql = "";
        int dbId = 0;

        if (horizontalClause != null) {
            sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                    + "VALUES (%d,'%d','%s','%s','%s', %s, %s )", tableId, 1, checkConstraint.getConstraintIdentifier().getIdentifier(), "CNN", unaryExpressionNN.getOperand().getIdentifier(), null, null);
            this.executeDDL(sql).close();

            sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                    + "VALUES (%d,'%d','%s','%s','%s', %s, %s )", tableId, 2, checkConstraint.getConstraintIdentifier().getIdentifier(), "CNN", unaryExpressionNN.getOperand().getIdentifier(), null, null);
            this.executeDDL(sql).close();

            if (horizontalClause.getFirstBoundary() != null) {
                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                        + "VALUES (%d,'%d','%s','%s','%s', %s, %s )", tableId, 3, checkConstraint.getConstraintIdentifier().getIdentifier(), "CNN", unaryExpressionNN.getOperand().getIdentifier(), null, null);
                this.executeDDL(sql).close();
            }
        } else {
            if (verticalClause != null) {
                String columnStmt = "SELECT database_id FROM Meta_Columns WHERE column_name = '" + unaryExpressionNN.getOperand().getIdentifier() + "' AND TABLE_ID = " + tableId;
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", columnStmt));
                try (Statement columnStatement = connectionDb1.createStatement();
                        ResultSet rs = columnStatement.executeQuery(columnStmt);) {

                    if (rs.next()) {
                        dbId = rs.getInt("database_id");
                    }
                }
            } else if (horizontalClause == null) {
                // Set dbId on 1, because no partition is avaiable
                dbId = 1;
            }

            sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute ) "
                    + "VALUES (%d,'%d','%s','%s','%s', %s, %s )", tableId, dbId, checkConstraint.getConstraintIdentifier().getIdentifier(), "CNN", unaryExpressionNN.getOperand().getIdentifier(), null, null);
            this.executeDDL(sql).close();
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="CheckBetween" >
    /**
     * Methode zum Verarbeiten eines Between-Constraints..
     *
     * @param checkConstraint Das constraint.
     * @throws FedException Weiterwerfen der FedException.
     * @throws SQLException Weiterwerfen der SQLException.
     */
    private void processCheckBetweenConstraint(fdbs.sql.parser.ast.constraint.CheckConstraint checkConstraint) throws FedException, SQLException {
        BetweenExpression betweenExpression = (BetweenExpression) checkConstraint.getCheckExpression();
        int dbId = 0;
        String sql = "";
        String minType = "VARCHAR(";
        String maxType = "VARCHAR(";
        if (betweenExpression.getMin() instanceof StringLiteral) {
            minType += betweenExpression.getMin().getIdentifier().length() + ")";
        } else {
            minType = "INTEGER";
        }

        if (betweenExpression.getMax() instanceof StringLiteral) {
            maxType += betweenExpression.getMax().getIdentifier().length() + ")";
        } else {
            maxType = "INTEGER";
        }

        sql = String.format("INSERT INTO Meta_Table_Constraints_Between (between_constraint_id, min_type, min_value, max_type, max_value) "
                + "VALUES (mtp_seq.nextval, '%s', '%s', '%s', '%s')", minType, betweenExpression.getMin().getIdentifier(), maxType, betweenExpression.getMax().getIdentifier());

        int constraintBetweenId = 0;
        this.executeDDL(sql).close();
        try (Statement sqlStatement = this.executeDDL("SELECT mtp_seq.currval FROM dual");
                ResultSet rs = sqlStatement.getResultSet()) {

            if (rs.next()) {
                constraintBetweenId = rs.getInt(1);
            }
        }
        if (horizontalClause != null) {
            sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, between_constraint_id ) "
                    + "VALUES (%d, '%d', '%s', '%s', '%s', %s, %s, %d)", tableId, 1, checkConstraint.getConstraintIdentifier().getIdentifier(), "CB", betweenExpression.getAttributeIdentifier().getIdentifier(), null, null, constraintBetweenId);
            this.executeDDL(sql).close();

            sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, between_constraint_id ) "
                    + "VALUES (%d, '%d', '%s', '%s', '%s', %s, %s, %d)", tableId, 2, checkConstraint.getConstraintIdentifier().getIdentifier(), "CB", betweenExpression.getAttributeIdentifier().getIdentifier(), null, null, constraintBetweenId);
            this.executeDDL(sql).close();

            if (horizontalClause.getFirstBoundary() != null) {
                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, between_constraint_id ) "
                        + "VALUES (%d, '%d', '%s', '%s', '%s', %s, %s, %d)", tableId, 3, checkConstraint.getConstraintIdentifier().getIdentifier(), "CB", betweenExpression.getAttributeIdentifier().getIdentifier(), null, null, constraintBetweenId);
                this.executeDDL(sql).close();
            }
        } else {
            if (verticalClause != null) {
                String columnStmt = "SELECT database_id FROM Meta_Columns WHERE column_name = '" + betweenExpression.getAttributeIdentifier().getIdentifier() + "' AND TABLE_ID = " + tableId;
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", columnStmt));
                try (Statement columnStatement = connectionDb1.createStatement();
                        ResultSet rs = columnStatement.executeQuery(columnStmt);) {

                    if (rs.next()) {
                        dbId = rs.getInt("database_id");
                    }
                }
            } else if (horizontalClause == null) {
                // Set dbId on 1, because no partition is avaiable
                dbId = 1;
            }

            sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, between_constraint_id ) "
                    + "VALUES (%d, '%d', '%s', '%s', '%s', %s, %s, %d)", tableId, dbId, checkConstraint.getConstraintIdentifier().getIdentifier(), "CB", betweenExpression.getAttributeIdentifier().getIdentifier(), null, null, constraintBetweenId);
            this.executeDDL(sql).close();
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="CheckBinary" >
    /**
     * Methode zum Verarbeiten eines Binary-Constraints.
     *
     * @param checkConstraint Das constraint.
     * @throws FedException Weiterwerfen der FedException.
     * @throws SQLException Weiterwerfen der SQLException.
     */
    private void processCheckBinaryConstraint(fdbs.sql.parser.ast.constraint.CheckConstraint checkConstraint) throws FedException, SQLException {
        BinaryExpression binaryExpression = (BinaryExpression) ((CheckConstraint) checkConstraint).getCheckExpression();
        String left = binaryExpression.getLeftOperand().getIdentifier();
        String right = binaryExpression.getRightOperand().getIdentifier();
        String binaryType = "c";
        int dbId = 0;
        String sql = "";
        int dbIdSecondColumn = 0;

        // If right operand is column
        if (dataTypeMap.get(right) != null) {
            binaryType = "a";
            sql = String.format("INSERT INTO Meta_Table_Constraints_Binary (binary_constraint_id, operator, binary_type, left_column_name, left_column_type, right_literal_value, right_literal_type ) "
                    + "VALUES(mtp_seq.nextval, '%s', '%s', '%s', '%s', '%s', '%s')", checkConstraint.getCheckExpression().getOperatorName(), binaryType, left, dataTypeMap.get(left), right, dataTypeMap.get(right));
        } else {
            sql = String.format("INSERT INTO Meta_Table_Constraints_Binary (binary_constraint_id, operator, binary_type, left_column_name, left_column_type, right_literal_value, right_literal_type ) "
                    + "VALUES(mtp_seq.nextval, '%s', '%s', '%s', '%s', '%s', '%s')", checkConstraint.getCheckExpression().getOperatorName(), binaryType, left, dataTypeMap.get(left), right, dataTypeMap.get(left));
        }

        // Die erstellte constraintBinaryId holen
        int constraintBinaryId = 0;
        this.executeDDL(sql).close();

        try (Statement sqlStatement = this.executeDDL("SELECT mtp_seq.currval FROM dual");
                ResultSet rs = sqlStatement.getResultSet()) {

            if (rs.next()) {
                constraintBinaryId = rs.getInt(1);
            }
        }

        if (binaryType.equals("c")) {
            if (horizontalClause != null) {
                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                        + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, 1, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                this.executeDDL(sql).close();

                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                        + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, 2, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                this.executeDDL(sql).close();

                if (horizontalClause.getFirstBoundary() != null) {
                    sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                            + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, 3, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                    this.executeDDL(sql).close();
                }
            } else {
                if (verticalClause != null) {
                    String columnStmt = "SELECT database_id FROM Meta_Columns WHERE column_name = '" + left + "' AND TABLE_ID = " + tableId;
                    Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", columnStmt));
                    try (Statement columnStatement = connectionDb1.createStatement();
                            ResultSet rs = columnStatement.executeQuery(columnStmt)) {

                        if (rs.next()) {
                            dbId = rs.getInt("database_id");
                        }
                        rs.close();
                    }
                } else {
                    dbId = 1;
                }

                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                        + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, dbId, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                this.executeDDL(sql).close();
            }
        } else if (binaryType.equals("a")) {
            if (horizontalClause != null) {
                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                        + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, 1, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                this.executeDDL(sql).close();

                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                        + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, 2, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                this.executeDDL(sql).close();

                if (horizontalClause.getFirstBoundary() != null) {
                    sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                            + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, 3, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                    this.executeDDL(sql).close();
                }
            } else if (verticalClause != null) {
                String columnStmt = "SELECT database_id FROM Meta_Columns WHERE column_name = '" + left + "' AND TABLE_ID = " + tableId;
                Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", columnStmt));
                try (Statement columnStatement = connectionDb1.createStatement();
                        ResultSet rs1 = columnStatement.executeQuery(columnStmt);) {

                    if (rs1.next()) {
                        dbId = rs1.getInt("database_id");
                    }
                    rs1.close();

                    // Falls zwei Spalten vorhanden sind, prüfen wo die zweite Spalte liegt
                    if (!binaryType.equals("c")) {
                        String columnStmt2 = "SELECT database_id FROM Meta_Columns WHERE column_name = '" + right + "'AND TABLE_ID = " + tableId;
                        Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", columnStmt2));
                        try (ResultSet rs = columnStatement.executeQuery(columnStmt2)) {
                            if (rs.next()) {
                                dbIdSecondColumn = rs.getInt("database_id");
                            }
                        }

                        // Die Spalten sind auf zwei verschiedenen Partitionen und diese kann nicht eindeutig zu einer Db zu geordnet werden
                        if (dbIdSecondColumn != dbId) {
                            dbId = 0;
                        }
                    }
                }

                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                        + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, dbId, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                this.executeDDL(sql).close();
            } else {
                sql = String.format("INSERT INTO Meta_Table_Constraints (table_id, database_id,constraint_name,constraint_type,constraint_attribute,constraint_table_ref,constraint_table_ref_attribute, binary_constraint_id ) "
                        + "VALUES (%d,'%d','%s','%s', %s, %s, %s, %d)", tableId, 1, checkConstraint.getConstraintIdentifier().getIdentifier(), "CC", null, null, null, constraintBinaryId);
                this.executeDDL(sql).close();
            }
        }
    }
    // </editor-fold>

    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Private Helper Functions" >
    /**
     * Methode zum löschen der lokalen Daten.
     */
    private void clearLocalData() {
        tables = new ArrayList();
        tablesByName.clear();
    }

    /**
     * Methode zum Ausführen einer DDL-Query ohne Rückgabe eines Ergebnisses.
     *
     * @param sql Die Anweisung
     */
    private Statement executeDDL(String sql) throws FedException {
        try {
            Statement statement = connectionDb1.createStatement();
            Logger.infoln(String.format("%s: Executing statement on database %s:%s", this.getClass().getSimpleName(), "1", sql));
            statement.executeUpdate(sql);
            return statement;
        } catch (SQLException ex) {
            Logger.error(String.format("%s: An error occurred while excuting DDL statement.", this.getClass().getSimpleName()), ex);
            throw new FedException(String.format("%s: An error occurred while excuting DDL statement.", this.getClass().getSimpleName()), ex);
        }
    }
    // </editor-fold>
}
