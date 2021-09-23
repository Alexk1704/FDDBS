package fdbs.sql.unifier;

import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuterSubTask;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.parser.ast.AST;
import java.util.ArrayList;
import java.util.HashMap;

public abstract class ExecuteUpdateUnifier {

    protected int linesAffected;

    protected SQLExecuterTask task;
    protected AST tree;
    protected final HashMap<Integer, ArrayList<Integer>> results;

    protected ExecuteUpdateUnifier(SQLExecuterTask task, AST tree) throws FedException {
        this.task = task;
        this.tree = tree;

        results = new HashMap<>();

        ArrayList<Integer> resultsDB1 = new ArrayList<>();
        for (SQLExecuterSubTask subTask : task.getDb1Statements()) {
            if (!subTask.isUpdate()) {
                throw new FedException(String.format("%s: Unexpected sql executer sub task result.", this.getClass().getSimpleName()));
            }
            resultsDB1.add(subTask.getUpdateResult());
        }
        results.put(1, resultsDB1);

        ArrayList<Integer> resultsDB2 = new ArrayList<>();
        for (SQLExecuterSubTask subTask : task.getDb2Statements()) {
            if (!subTask.isUpdate()) {
                throw new FedException(String.format("%s: Unexpected sql executer sub task result.", this.getClass().getSimpleName()));
            }
            resultsDB2.add(subTask.getUpdateResult());
        }
        results.put(2, resultsDB2);

        ArrayList<Integer> resultsDB3 = new ArrayList<>();
        for (SQLExecuterSubTask subTask : task.getDb3Statements()) {
            if (!subTask.isUpdate()) {
                throw new FedException(String.format("%s: Unexpected sql executer sub task result.", this.getClass().getSimpleName()));
            }
            resultsDB3.add(subTask.getUpdateResult());
        }
        results.put(3, resultsDB3);
    }

    protected abstract int unifyResult() throws FedException;
}
