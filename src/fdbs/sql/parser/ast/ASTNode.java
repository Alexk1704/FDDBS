package fdbs.sql.parser.ast;

import java.util.ArrayList;
import java.util.List;

import fdbs.sql.parser.ast.visitor.ASTVisitor;
import fdbs.util.logger.*;

/**
 * An abstract syntax tree node in an abstract syntrax tree.
 */
public abstract class ASTNode {

    protected ASTNode parent = null;
    protected String identifier = new String();
    protected List<ASTNode> childrenList = new ArrayList<ASTNode>();

    /**
     * The Constructor for an abstract syntax tree node.
     */
    public ASTNode() {
    }

    /**
     * Gets the parent of the abstract syntrax tree node.
     *
     * @return Returns the parent or null when no parent exists.
     */
    public ASTNode getParent() {
        return parent;
    }

    /**
     * Gets the identifier of this node. Mostly just have leaves of an abstract
     * syntax tree a identifier.
     *
     * @return Returns a identifier or a empty string when no identifier exists.
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Sets the identifier of this node.
     *
     * @param identifier The identifier of this node.
     */
    protected void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    /**
     * Adds a child to this node.
     *
     * @param child A child of this node.
     */
    protected void addChild(ASTNode child) {
        child.parent = this;
        this.childrenList.add(child);
    }

    /**
     * Adds a child node into a specific index.
     *
     * @param index The index of this child.
     * @param child A child of this node.
     */
    protected void addChild(int index, ASTNode child) {
        child.parent = this;
        this.childrenList.add(index, child);
    }

    /**
     * Adds a collection of child's to this node.
     *
     * @param childs A collection of child's of this node.
     */
    protected void addChilds(List<? extends ASTNode> childs) {
        for (ASTNode child : childs) {
            this.addChild(child);
        }
    }

    /**
     * Accepts An abstract syntax tree visitor and traversed with the visitor
     * through the tree.
     *
     * @param visitor An abstract syntax tree visitor.
     * @throws Exception
     */
    public final void accept(ASTVisitor visitor) throws Exception {
        Logger.traceln(String.format("%s: Will visit %s node.", visitor.getClass().getSimpleName(), this.getClass().getSimpleName()));
        visitor.willVisit(this);

        Logger.traceln(String.format("%s: Visiting %s.", visitor.getClass().getSimpleName(), this.getClass().getSimpleName()));
        if (visitor.visit(this)) {
            for (ASTNode childNode : this.childrenList) {
                childNode.accept(visitor);
            }
        }

        visitor.didVisit(this);
        Logger.traceln(String.format("%s: Did visit %s.", visitor.getClass().getSimpleName(), this.getClass().getSimpleName()));
    }

    /**
     * Serialized this node and his children to a pretty printed tree.
     *
     * @param indent The indent of this node in the abstract syntax tree.
     * @param last Is this node the last node?
     * @return Returns a pretty printed abstract syntax tree with this node as
     * root node.
     */
    public String printPretty(String indent, boolean last) {
        String out = "%n" + indent;

        if (last) {
            out += "\\-";
            indent += "  ";
        } else {
            out += "|-";
            indent += "| ";
        }

        String name = this.getClass().getSimpleName();
        if (this.getIdentifier() != null && !this.getIdentifier().trim().isEmpty()) {
            name += ": " + this.getIdentifier();
        }

        out += name;

        for (int i = 0; i < this.childrenList.size(); i++) {
            out += this.childrenList.get(i).printPretty(indent, i == this.childrenList.size() - 1);
        }

        return out;
    }

    /**
     * Gets all children of this node.
     *
     * @return Returns all children of this node.
     */
    public List<ASTNode> getChildren() {
        return this.childrenList;
    }
}
