package fdbs.sql.unifier;

import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.parser.ast.AST;
import fdbs.util.logger.Logger;

public class DDLStmtUnifier extends ExecuteUpdateUnifier {

    private boolean noErrorState = true;

    public DDLStmtUnifier(SQLExecuterTask task, AST tree) throws FedException {
        super(task, tree);
    }

    //return 0 if the DDL Statements is ok for every db
    //return 1 if an error is ocurred in any db
    @Override
    protected int unifyResult() throws FedException {
        //set linesAffected to 1 for error state
        linesAffected = 1;

        if (!results.isEmpty()) {
            for (int key : results.keySet()) {
                for (int number : results.get(key)) {
                    if (number != 0) {
                        noErrorState = false;
                        Logger.debugln(String.format("%s : other value than 0 found, set noErrorState to false", this.getClass().getSimpleName()));
                    }
                }
            }
        } else {
            throw new FedException(String.format("%s : Unexpected empty results.", this.getClass().getSimpleName()));
        }

        return (noErrorState == true) ? 0 : 1;
    }
}
