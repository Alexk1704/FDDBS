package fdbs.sql.unifier;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.ast.statement.*;
import fdbs.util.logger.Logger;

public class ExecuteUpdateUnifierManager implements ExecuteUpdateUnifierManagerInterface {

    private int linesUpdated;
    protected MetadataManager metadataManager;
    protected FedConnection fedConnection;

    public ExecuteUpdateUnifierManager(FedConnection fedConnection) throws FedException {
        linesUpdated = 0;
        this.metadataManager = MetadataManager.getInstance(fedConnection);
        this.fedConnection = fedConnection;
    }

    /**
     * This method unifies all executeUpdate-Statements of a given Hashmap to a
     * single Integer
     *
     * @param tree AST
     * @param task An Executer executed task.
     * @return int of affected rows for INSERT, UPDATE and DELETE Statements, -1
     * for Empty Hashmap - or 0 for DDL-Statements (CREATE, DROP)
     * @throws fdbs.sql.FedException
     */
    @Override
    public int unifyUpdateResult(AST tree, SQLExecuterTask task) throws FedException {

        ASTNode statement = tree.getRoot();

        if (statement instanceof InsertStatement) {
            Logger.infoln(String.format("%s : Start processing insert statement with DMLStmtUnifier", this.getClass().getSimpleName()));
            DMLStmtUnifier insertUnifier = new DMLStmtUnifier(task, tree, fedConnection);
            linesUpdated = insertUnifier.unifyResult();
            Logger.infoln(String.format("%s : Finished processing insert statement with DMLStmtUnifier", this.getClass().getSimpleName()));
        } else if (statement instanceof UpdateStatement) {
            Logger.infoln(String.format("%s : Start processing update statement with DMLStmtUnifier", this.getClass().getSimpleName()));
            DMLStmtUnifier updateUnifier = new DMLStmtUnifier(task, tree, fedConnection);
            linesUpdated = updateUnifier.unifyResult();
            Logger.infoln(String.format("%s : Finished processing update statement with DMLStmtUnifier", this.getClass().getSimpleName()));
        } else if (statement instanceof CreateTableStatement) {
            Logger.infoln(String.format("%s : Start processing create statement with DDLStmtUnifier", this.getClass().getSimpleName()));
            DDLStmtUnifier createUnifier = new DDLStmtUnifier(task, tree);
            linesUpdated = createUnifier.unifyResult();
            Logger.infoln(String.format("%s : Finished processing create statement with DMLStmtUnifier", this.getClass().getSimpleName()));
        } else if (statement instanceof DropTableStatement) {
            Logger.infoln(String.format("%s : Start processing drop table statement with DDLStmtUnifier", this.getClass().getSimpleName()));
            DDLStmtUnifier dropUnifier = new DDLStmtUnifier(task, tree);
            linesUpdated = dropUnifier.unifyResult();
            Logger.infoln(String.format("%s : Finished processing drop table statement with DMLStmtUnifier", this.getClass().getSimpleName()));
        } else if (statement instanceof DeleteStatement) {
            Logger.infoln(String.format("%s : Start processing delete statement with DMLStmtUnifier", this.getClass().getSimpleName()));
            DMLStmtUnifier deleteUnifier = new DMLStmtUnifier(task, tree, fedConnection);
            linesUpdated = deleteUnifier.unifyResult();
            Logger.infoln(String.format("%s : Finished processing delete statement with DMLStmtUnifier", this.getClass().getSimpleName()));
        } else {
            throw new FedException(String.format("%s : Statement \"%s\" has a unknown statement type.", this.getClass().getSimpleName(), statement.getClass().getName()));
        }

        return linesUpdated;
    }
}
