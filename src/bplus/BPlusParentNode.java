package bplus;

import java.util.ArrayList;

public class BPlusParentNode<T extends Comparable<T>> extends BPlusNode<T> {

    private ArrayList<NodePointer<T>> children;

    /**
     * Creates a BPlusNode which is a parent of other nodes, either
     * leaf nodes or other parent nodes
     * @param pageSize The pageSize of the tree
     * @param value The value of this node, used to determine which branch
     *              of the tree to follow when getting/inserting records
     * @param children The children of this parent node
     */
    public BPlusParentNode(int pageSize, T value, ArrayList<NodePointer<T>> children) {
        super(pageSize, value);
        this.children = children;
    }

    /**
     * Finds the appropriate child node and sends the record down to be inserted
     * @param rp The RecordPointer to insert
     * @return `true` if the record was inserted successfully; `false` if a record with that
     * value already exists in the table
     */
    @Override
    public boolean insertRecord(RecordPointer<T> rp) {
        for (NodePointer<T> child : children) {
            if (child.isGreaterThan(rp)) {
                //TODO: Handle restructuring tree
                return child.getPtr().insertRecord(rp);
            }
        }
        // The value of the last child should be null, which means `isGreaterThan` should
        // always return `true` and the record will be inserted there. If that doesn't
        // happen, something is broken
        throw new InternalError("INSERT: Record " + rp +
                " was not searched for in any child of " + this);
    }

    @Override
    public boolean deleteRecord(T value) {
        for (NodePointer<T> child : children) {
            if (child.isGreaterThan(value)) {
                //TODO: Handle restructuring tree
                return child.getPtr().deleteRecord(value);
            }
        }
        // See insertRecord() for why this error is here
        throw new InternalError("DELETE: Value " + value +
                " was not searched for in any child of " + this);
    }

    @Override
    public boolean contains(T value) {
        for (NodePointer<T> child : children) {
            if (child.isGreaterThan(value)) {
                return child.getPtr().contains(value);
            }
        }
        // See insertRecord() for why this error is here
        throw new InternalError("CONTAINS: Value " + value +
                " was not searched for in any child of " + this);
    }

    @Override
    public RecordPointer<T> get(T value) {
        for (NodePointer<T> child : children) {
            if (child.isGreaterThan(value)) {
                return child.getPtr().get(value);
            }
        }
        // See insertRecord() for why this error is here
        throw new InternalError("GET: Value " + value +
                " was not searched for in any child of " + this);
    }
}
