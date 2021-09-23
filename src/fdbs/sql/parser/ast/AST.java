package fdbs.sql.parser.ast;

/**
 * A abstract syntax tree represent a parsed sql statement.
 */
public class AST {

    private ASTNode root;

    /**
     * The Constructor for an abstract syntax tree.
     */
    public AST() {

    }

    /**
     * Gets the root node of the abstract syntax tree.
     *
     * @return Returns the root node of the abstract syntax tree.
     */
    public ASTNode getRoot() {
        return this.root;
    }

    /**
     * Sets the root node for the abstract syntax tree.
     *
     * @param node An abstract syntax tree node.
     */
    public void setRoot(ASTNode node) {
        this.root = node;
    }

    /**
     * Serialized an abstract syntax tree to a pretty printed tree.
     *
     * @return Returns a pretty printed abstract syntax tree.
     */
    public String printPretty() {
        return this.root.printPretty("", true);
    }
}
