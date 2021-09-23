package fdbs.sql.resolver;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ast.AST;
import fdbs.sql.parser.ast.ASTNode;
import fdbs.sql.parser.ast.statement.*;

/**
 *
 * This class acts as a manager and handles the AST tree to find out which
 * StatementResolver needs to be instantiated, it calls the method of the
 * implemented sub class and passes the ASTNode to the specific resolver to
 * create a resolved statement. Its also an abstract class and implements an
 * interface for other modules.
 */
public class StatementResolverManager {

    protected MetadataManager metadataManager;
    protected FedConnection fedConnection;

    public StatementResolverManager(FedConnection fedConnection) throws FedException {
        this.metadataManager = MetadataManager.getInstance(fedConnection);
        this.fedConnection = fedConnection;
    }

    /**
     * This method finds out which StatementResolver needs to be instantiated
     * and returns a ResolvedStatement.
     *
     * @param tree, AST tree from the sql parser
     * @return resStatement, ResolvedStatement which was produced by a specific
     * factory
     * @throws fdbs.sql.FedException
     */
    public SQLExecuterTask resolveStatement(AST tree) throws FedException {
        SQLExecuterTask executerTask;
        StatementResolverManager statementResolver;

        ASTNode statement = tree.getRoot();
        if (statement instanceof CreateTableStatement) {
            statementResolver = new CreateStatementResolver(fedConnection);
        } else if (statement instanceof DropTableStatement) {
            statementResolver = new DropStatementResolver(fedConnection);
        } else if (statement instanceof InsertStatement) {
            statementResolver = new InsertStatementResolver(fedConnection);
        } else if (statement instanceof UpdateStatement) {
            statementResolver = new UpdateStatementResolver(fedConnection);
        } else if (statement instanceof DeleteStatement) {
            statementResolver = new DeleteStatementResolver(fedConnection);
        } else {
            throw new FedException(String.format("%s: Statement from type \"%s\" is not supported.", this.getClass().getSimpleName(), statement.getClass().getName()));
        }

        executerTask = statementResolver.resolveStatement(tree);

        return executerTask;
    }
}
