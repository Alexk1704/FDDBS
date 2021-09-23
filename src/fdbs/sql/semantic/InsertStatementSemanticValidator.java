package fdbs.sql.semantic;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.Column;
;
import fdbs.sql.meta.Table;
import fdbs.sql.parser.ast.*;
import fdbs.sql.parser.ast.literal.Literal;
import fdbs.sql.parser.ast.literal.NullLiteral;
import fdbs.sql.parser.ast.statement.InsertStatement;
import fdbs.util.logger.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A insert statement semantic validator which makes inter alia type checks and
 * integrity checks.
 */


public class InsertStatementSemanticValidator extends SemanticValidator {

    public InsertStatementSemanticValidator(FedConnection fedConnection) throws FedException {
        super(fedConnection);
    }

    @Override
    public void willVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Start insert statement semantic validation.", this.getClass().getSimpleName()));
    }

    @Override
    public boolean visit(ASTNode node) throws SemanticValidationException, FedException {
        boolean visitChilds = true;

        if (node instanceof InsertStatement) {
            InsertStatement insertStatement = (InsertStatement) node;

            Table table = processTableName(insertStatement.getTableIdentifier().getIdentifier(), true);
            HashMap<String, String> attributeNameValueMapping = processValues(table, insertStatement.getValues());
            checkIntegrityConditions(table.getConstraints(), table, attributeNameValueMapping, false, null);

            visitChilds = false;
        }

        return visitChilds;
    }

    @Override
    public void didVisit(ASTNode node) throws SemanticValidationException {
        Logger.infoln(String.format("%s: Finished insert statement semantic validation.", this.getClass().getSimpleName()));
    }

    // <editor-fold defaultstate="collapsed" desc="Private Helper Functions">
    private HashMap<String, String> processValues(Table table, List<Literal> values) throws SemanticValidationException {
        HashMap<String, String> attributeNameValueMapping = new HashMap<>();

        ArrayList<Column> columns = table.getColumns();

        if (values.size() < columns.size()) {
            throw new SemanticValidationException(String.format("%s: The number of values is insufficient.", this.getClass().getSimpleName()));
        } else if (values.size() > columns.size()) {
            throw new SemanticValidationException(String.format("%s: The number of values is too many.", this.getClass().getSimpleName()));
        }

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            Literal value = values.get(i);
            String columnName = column.getAttributeName();

            if (value instanceof NullLiteral && !table.isColumnNullAble(column.getAttributeName())) {
                throw new SemanticValidationException(String.format("%s: Null check constraint violation - The value from the attribute \"%s.%s\" is null.", this.getClass().getSimpleName(), table.getAttributeName(), columnName));
            }

            if (!isValueTypeOfColumnOrCanBeConverted(column.getType(), value)) {
                throw new SemanticValidationException(String.format("%s: The type from the attribute \"%s\" is not equals the type of the setting value and the type of the setting value can not be converted into the attribute type.", this.getClass().getSimpleName(), columnName));
            }

            if (value instanceof NullLiteral) {
                attributeNameValueMapping.put(columnName, null);
            } else {
                attributeNameValueMapping.put(columnName, value.getIdentifier());
            }

        }

        return attributeNameValueMapping;
    }

    // </editor-fold>
}
