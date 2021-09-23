package fdbs.sql.parser.ast.visitor;

import fdbs.sql.parser.ast.ASTNode;

/**
 * An abstract syntax tree visitor which can be used to traverse through an abstract syntax tree.
 */
public abstract class ASTVisitor {

    /**
     * The Constructor for an abstract syntax tree visitor.
     */
    public ASTVisitor() {
    }

    /**
     * The willVisit method is called when an abstract syntax tree node should be visit while traversing.
     * @param node The abstract syntax tree node which should be visit.
     * @throws Exception
     */
    public abstract void willVisit(ASTNode node) throws Exception;

    /**
     * The visit method is called when an abstract syntax tree node will be visit while traversing.
     * @param node The abstract syntax tree node which will be visit.
     * @return Returns true when the children of the abstract syntax tree node should be visit.
     * @throws Exception
     */
    public boolean visit(ASTNode node) throws Exception {
        return true;
    }

    /**
     * The didVisit method is called when an abstract syntax tree node and his children did be visit while traversing.
     * @param node The abstract syntax tree node which did be visit.
     * @throws Exception
     */
    public abstract void didVisit(ASTNode node) throws Exception;
}
