package fdbs.sql.unifier;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.meta.PartitionType;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.ast.statement.DeleteStatement;
import fdbs.sql.parser.ast.statement.InsertStatement;
import fdbs.sql.parser.ast.statement.UpdateStatement;

public class DMLStmtUnifier extends ExecuteUpdateUnifier {

    protected MetadataManager metadataManager;
    protected FedConnection fedConnection;

    public DMLStmtUnifier(SQLExecuterTask task, AST tree, FedConnection fedConnection) throws FedException {
        super(task, tree);
        this.metadataManager = MetadataManager.getInstance(fedConnection);
        this.fedConnection = fedConnection;
    }

    /**
     * used to unify a hashmap with list of integer values for an DML (Insert,
     * Update, Delete) statmentent
     *
     * @return int of affected rows or -1 if there is an empty Hashmap
     * @throws fdbs.sql.FedException
     */
    @Override
    protected int unifyResult() throws FedException {
        linesAffected = 0;
        ASTNode node = tree.getRoot();

        if (node instanceof InsertStatement) {
            InsertStatement insertStatement = (InsertStatement) node;
            String tableName = insertStatement.getTableIdentifier().getIdentifier();
            Table table = metadataManager.getTable(tableName);

            linesAffected = getHighest(table.getPartitionType() == PartitionType.Vertical);
            
        } else if (node instanceof DeleteStatement || node instanceof UpdateStatement) {
            String tableName = "";

            if (node instanceof DeleteStatement) {
                DeleteStatement deleteStatement = (DeleteStatement) node;
                tableName = deleteStatement.getTableIdentifier().getIdentifier();
            } else {
                UpdateStatement updateStatement = (UpdateStatement) node;
                tableName = updateStatement.getTableIdentifier().getIdentifier();
            }

            Table table = metadataManager.getTable(tableName);

            if (table.getPartitionType() == PartitionType.Horizontal) {
                linesAffected = getHorizontalSum();
            } else {
                linesAffected = getHighest(false);
            }
        }

        return linesAffected;
    }

    private int getHighest(boolean checkAllResultsAreTheSame) throws FedException {
        int linesAffectedPrivate = 0;
        boolean didProcessFirstValue = false;

        if (!results.isEmpty()) {
            for (int key : results.keySet()) {              
                for (int number : results.get(key)) {
                    
                    //check only needed for Insert Statements
                    if (checkAllResultsAreTheSame && didProcessFirstValue && !(linesAffectedPrivate == number)) {
                        throw new FedException(String.format("%s : Unexpected results. Affected lines for each database are not equal.", this.getClass().getSimpleName()));
                    }

                    if (number > linesAffectedPrivate) {
                        linesAffectedPrivate = number;
                    }

                    didProcessFirstValue = true;
                }
            }
        } else {
            throw new FedException(String.format("%s : Unexpected empty results.", this.getClass().getSimpleName()));
        }

        return linesAffectedPrivate;
    }

    private int getHorizontalSum() throws FedException {
        int linesAffectedPrivate = 0;

        if (!results.isEmpty()) {
            for (int key : results.keySet()) {
                for (int number : results.get(key)) {
                    linesAffectedPrivate += number;
                }
            }
        } else {
            throw new FedException(String.format("%s : Unexpected empty results.", this.getClass().getSimpleName()));
        }

        return linesAffectedPrivate;
    }
}
