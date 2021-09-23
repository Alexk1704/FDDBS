package fdbs.sql.semantic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.Column;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.*;
import fdbs.sql.parser.ast.clause.WhereClause;
import fdbs.sql.parser.ast.literal.Literal;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.statement.UpdateStatement;
import fdbs.util.logger.Logger;
import java.util.HashMap;

/**
 * A update statement semantic validator which makes inter alia type checks and
 * integrity checks.
 */
public class UpdateStatementSemanticValidator extends SemanticValidator {

    public UpdateStatementSemanticValidator(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public void willVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Start update statement semantic validation.", this.getClass().getSimpleName()));
    }

    @Override
    public boolean visit(ASTNode node) throws SemanticValidationException, FedException {
        boolean visitChilds = true;

        if (node instanceof UpdateStatement) {
            UpdateStatement updateStatement = (UpdateStatement) node;

            Table table = processTableName(updateStatement.getTableIdentifier().getIdentifier(), true);

            checkSetAttribute(table, updateStatement.getAttributeIdentifier().getIdentifier(), updateStatement.getConstant());

            WhereClause whereClause = updateStatement.getWhereClause();
            if (whereClause != null) {
                checkWhereClause(new HashMap<String, Table>() {
                    {
                        put(table.getAttributeName(), table);
                    }
                }, whereClause);
            }

            checkIntegrityConditions(updateStatement, table);

            visitChilds = false;
        }

        return visitChilds;
    }

    @Override
    public void didVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Finished update statement semantic validation.", this.getClass().getSimpleName()));
    }

    // <editor-fold defaultstate="collapsed" desc="Private Helper Functions">
    private void checkSetAttribute(Table table, String attributeName, Literal constant) throws SemanticValidationException {
        Column column = table.getColumn(attributeName);
        if (column == null) {
            throw new SemanticValidationException(String.format("%s: The attribute \"%s\" does not exists in the table \"%s\".", this.getClass().getSimpleName(), attributeName, table.getAttributeName()));
        }

        if (constant instanceof NullLiteral && !table.isColumnNullAble(attributeName)) {
            throw new SemanticValidationException(String.format("%s: Null check constraint violation - The value from the attribute \"%s.%s\" is null.", this.getClass().getSimpleName(), table.getAttributeName(), column.getAttributeName()));
        }

        if (!isValueTypeOfColumnOrCanBeConverted(column.getType(), constant)) {
            throw new SemanticValidationException(String.format("%s: The type from the attribute \"%s\" is not equals the type of the setting value and the type of the setting value can not be converted into the attribute type.", this.getClass().getSimpleName(), attributeName));
        }
    }

    private void checkIntegrityConditions(UpdateStatement updateStatement, Table table)
            throws SemanticValidationException, FedException {
        HashMap<String, String> attributeNameValueMapping = new HashMap<>();
        if (updateStatement.getConstant() instanceof NullLiteral) {
            attributeNameValueMapping.put(updateStatement.getAttributeIdentifier().getIdentifier(), null);
        } else {
            attributeNameValueMapping.put(updateStatement.getAttributeIdentifier().getIdentifier(), updateStatement.getConstant().getIdentifier());
        }

        checkIntegrityConditions(table.getConstraints(), table, attributeNameValueMapping, true, updateStatement.getWhereClause());
    }

    // </editor-fold>
}
