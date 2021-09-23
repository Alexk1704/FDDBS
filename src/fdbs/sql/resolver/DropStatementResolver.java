package fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.statement.DropTableStatement;
import fdbs.util.logger.Logger;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 * Factory sub-class which resolves the statement from the AST node and creates
 * a specific ResolvedStatement object.
 */
public class DropStatementResolver extends StatementResolverManager {

    /**
     * Constructor for a Drop Statement Resolver
     *
     * @param fedConnection
     * @throws FedException
     */
    public DropStatementResolver(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    /**
     * This method creates a ResolvedStatement object and therefor runs all the
     * necessary methods to prepare the ASTnode statement and convert it into a
     * data structure containing executable sub-statements for all targeted
     * databases to pass to the executer module.
     *
     * @param tree, AST
     * @return resolvedStatement, ResolvedStatement object
     * @throws fdbs.sql.FedException
     */
    @Override
    public SQLExecuterTask resolveStatement(AST tree) throws FedException {
        DropTableStatement dropTableStatement = (DropTableStatement) tree.getRoot();
        String tableName = dropTableStatement.getTableIdentifier().getIdentifier();
        Table table = metadataManager.getTable(tableName);

        String executingStatement;
        if (dropTableStatement.hasCascadeConstraintsExtension()) {
            executingStatement = String.format("DROP TABLE %s CASCADE CONSTRAINTS", tableName);
        } else {
            executingStatement = String.format("DROP TABLE %s", tableName);
        }

        SQLExecuterTask executerTask = new SQLExecuterTask();
        for (Entry<Integer, LinkedHashMap<String, Column>> entry : table.getDatabaseRelatedColumns().entrySet()) {
            if (!entry.getValue().isEmpty()) {
                Integer dbId = entry.getKey();
                executerTask.addSubTask(executingStatement, true, dbId);
                Logger.infoln(String.format("%s: Added following statement as sub-task to DB%s:%n%s", this.getClass().getSimpleName(), dbId, dropTableStatement));
            }
        }

        metadataManager.handleQuery(tree);
        return executerTask;
    }
}
