package bplus;

public abstract class BPlusNode<T extends Comparable<T>> {

    private final int pageSize;
    protected T value;

    /**
     * Creates a node in a BPlusTree
     * @param pageSize The page size for the BPlusTree (i.e. the space for
     *                 storing records within a node)
     */
    public BPlusNode(int pageSize, T value) {
        this.pageSize = pageSize;
        this.value = value;
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

    /**
     * Checks if the value in this pointer is greater than the value in a given RecordPointer
     * @param rp The RecordPointer being compared
     * @return `true` if the RecordPointer contains a value which is less than the value
     * in this pointer, or if this pointer's value is null; `false` if the value in
     * the RecordPointer is greater than or equal to the value in this pointer
     */
    public boolean isGreaterThan(RecordPointer<T> rp) {
        if (value == null) {
            return true;
        }
        return value.compareTo(rp.getValue()) > 0;
    }

    /**
     * Checks if the value in this pointer is greater than another value
     * @param o The value being compared to this RecordPointer
     * @return `true` if the value is less than this pointer's value, or if
     * this pointer has a null value; `false` if the value is greater than
     * or equal to the value in this pointer
     */
    public boolean isGreaterThan(T o) {
        if (value == null) {
            return true;
        }
        return value.compareTo(o) > 0;
    }
}
