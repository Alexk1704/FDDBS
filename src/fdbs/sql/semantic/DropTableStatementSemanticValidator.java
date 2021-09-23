package fdbs.sql.semantic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.parser.ast.*;
import fdbs.sql.parser.ast.statement.DropTableStatement;
import fdbs.util.logger.Logger;

/**
 * A drop table statement semantic validator which makes inter alia type checks
 * and integrity checks.
 */
public class DropTableStatementSemanticValidator extends SemanticValidator {

    public DropTableStatementSemanticValidator(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public void willVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Start drop table statement semantic validation.", this.getClass().getSimpleName()));
    }

    @Override
    public boolean visit(ASTNode node) throws SemanticValidationException {
        boolean visitChilds = true;

        if (node instanceof DropTableStatement) {
            String tableName = ((DropTableStatement) node).getTableIdentifier().getIdentifier();
            processTableName(tableName, true);

            visitChilds = false;
        }

        return visitChilds;
    }

    @Override
    public void didVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Finished drop table statement semantic validation.", this.getClass().getSimpleName()));
    }
}
