package fdbs.sql.semantic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.*;
import fdbs.sql.parser.ast.clause.WhereClause;
import fdbs.sql.parser.ast.statement.DeleteStatement;
import fdbs.util.logger.Logger;
import java.util.HashMap;

/**
 * A delete statement semantic validator which makes inter alia type checks and
 * integrity checks.
 */
public class DeleteStatementSemanticValidator extends SemanticValidator {

    public DeleteStatementSemanticValidator(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public void willVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Start delete statement semantic validation.", this.getClass().getSimpleName()));
    }

    @Override
    public boolean visit(ASTNode node) throws SemanticValidationException {
        boolean visitChilds = true;

        if (node instanceof DeleteStatement) {
            DeleteStatement deleteStatement = (DeleteStatement) node;
            String tableName = deleteStatement.getTableIdentifier().getIdentifier();
            Table table = processTableName(tableName, true);

            WhereClause whereClause = deleteStatement.getWhereClause();
            if (whereClause != null) {
                checkWhereClause(new HashMap<String, Table>() {
                    {
                        put(table.getAttributeName(), table);
                    }
                }, whereClause);
            }

            visitChilds = false;
        }

        return visitChilds;
    }

    @Override
    public void didVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Finished delete statement semantic validation.", this.getClass().getSimpleName()));
    }
}
