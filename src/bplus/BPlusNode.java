package bplus;

import tableData.Bufferable;
import tableData.TableSchema;

public abstract class BPlusNode<T extends Comparable<T>> extends Bufferable {

    protected final TableSchema schema;

    /**
     * Creates a node in a BPlusTree
     * @param ts The TableSchema for the table this BPlus tree indexes into
     * @param pointer The pointer to this node within the BPlus file
     */
    public BPlusNode(TableSchema ts, long pointer) {
        schema = ts;
        this.number = pointer;
    }

    /**
     * Inserts a record, either into the node itself or a child
     * @param rp The RecordPointer being inserted
     * @return `true` if the record was inserted successfully; `false` if a record with that
     * value already exists in the table
     */
    public abstract boolean insertRecord(RecordPointer<T> rp);

    /**
     * Removes a record from the tree
     * @param value The value of the RecordPointer to remove
     * @return `true` if the record was removed from the tree; `false` if a record with that
     * value did not exist in the table
     */
    public abstract boolean deleteRecord(T value);

    /**
     * Checks if a record containing a given value exists within the tree
     * @param value The value being searched for
     * @return `true` if a record with the given key value exists; `false` otherwise
     */
    public abstract boolean contains(T value);

    /**
     * Gets the RecordPointer for a given value.
     * @param value The value whose RecordPointer is being searched for
     * @return If found, the RecordPointer for that value; `null` if record does not
     * exist in the tree
     */
    public abstract RecordPointer<T> get(T value);

    public String getTableName() {
        return schema.name;
    }
}
