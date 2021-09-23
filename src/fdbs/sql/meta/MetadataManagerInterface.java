package fdbs.sql.meta;

import fdbs.sql.FedException;
import java.util.ArrayList;
import java.util.HashMap;
import fdbs.sql.parser.ast.AST;
import java.sql.SQLException;

public interface MetadataManagerInterface {

    String getColumnType(String tableName, String columnName);

    Column getColumn(String tableName, String columnName);

    ArrayList<Column> getColumns(String tableName);

    HashMap<String, Column> getColumnsByDatabase(String tableName, int db);

    ArrayList<Column> getAllColumns();

    ArrayList<Table> getAllTables();

    Table getTable(String tableName);

    ArrayList<Constraint> getAllConstraints();

    ArrayList<Constraint> getConstraintsByTable(String tableName);

    Constraint getConstraint(String tableName, int db, String constraintName);

    ArrayList<Constraint> getConstraints(String tableName, int db);

    PartitionType getPartitionType(String tableName);

    ArrayList<HorizontalPartition> getHorizontalPartition(String tableName);

    HashMap<String, Column> getVerticalPartition(String tableName);

    void handleQuery(AST tree) throws FedException;

    void deleteMetadata() throws FedException;
}
