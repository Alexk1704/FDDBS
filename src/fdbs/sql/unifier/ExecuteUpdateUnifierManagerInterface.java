package fdbs.sql.unifier;

import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.parser.ast.AST;

public interface ExecuteUpdateUnifierManagerInterface {

    int unifyUpdateResult(AST tree, SQLExecuterTask task) throws FedException;
}
