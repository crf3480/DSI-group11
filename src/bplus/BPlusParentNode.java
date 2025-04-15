package bplus;

import tableData.TableSchema;

import java.io.IOException;
import java.util.ArrayList;

class BPlusParentNode<T extends Comparable<T>> extends BPlusNode<T> {

    private ArrayList<BPlusNodePointer<T>> children;

    /**
     * Creates a BPlusNode which is a parent of other nodes, either
     * leaf nodes or other parent nodes
     * @param schema The TableSchema of the  pageSize of the tree
     * @param pointer The pointer to this node in the BPlus file
     * @param children The children of this parent node
     */
    public BPlusParentNode(TableSchema schema, long pointer, ArrayList<BPlusNodePointer<T>> children) {
        super(schema, pointer);
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
        for (BPlusNodePointer<T> child : children) {
            if (child.isGreaterThan(rp)) {
                //TODO: Handle restructuring tree
                //TODO: child.insertRecord(rp);
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
        for (BPlusNodePointer<T> child : children) {
            if (child.isGreaterThan(value)) {
                //TODO: child.delete(value)
                //TODO: Handle restructuring tree
            }
        }
        // See insertRecord() for why this error is here
        throw new InternalError("DELETE: Value " + value +
                " was not searched for in any child of " + this);
    }

    @Override
    public boolean contains(T value) {
        for (BPlusNodePointer<T> child : children) {
            if (child.isGreaterThan(value)) {
                //TODO: child.contains(value)
            }
        }
        // See insertRecord() for why this error is here
        throw new InternalError("CONTAINS: Value " + value +
                " was not searched for in any child of " + this);
    }

    @Override
    public RecordPointer<T> get(T value) {
        for (BPlusNodePointer<T> child : children) {
            if (child.isGreaterThan(value)) {
                //TODO: child.get(value)
            }
        }
        // See insertRecord() for why this error is here
        throw new InternalError("GET: Value " + value +
                " was not searched for in any child of " + this);
    }

    @Override
    public void save() throws IOException {
        //TODO: Serialize node
    }
}
