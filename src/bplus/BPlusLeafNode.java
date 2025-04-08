package bplus;

import java.util.ArrayList;

public class BPlusLeafNode<T extends Comparable<T>> extends BPlusNode<T> {

    public ArrayList<RecordPointer<T>> records;
    //TODO: This attribute exists, but it is never read/updated
    public BPlusNode<T> nextLeaf;

    /**
     * Creates a Leaf node which is intended for the root of a B+ tree. It
     * has no records, and by extension has no value
     * @param pageSize The pageSize for the tree nodes
     */
    public BPlusLeafNode(int pageSize) {
        super(pageSize, null);
        this.records = new ArrayList<>();
    }

    /**
     * Creates a Leaf node for a B+ tree from a collection of RecordPointers
     * @param pageSize The pageSize for the tree nodes
     * @param value The value of this node, used by the parent to determine which node to
     *              explore when finding/inserting records
     * @param records The list of records stored in this node
     * @param nextLeaf The next leaf in the tree. `null` if this is the final leaf
     */
    public BPlusLeafNode(int pageSize, T value, ArrayList<RecordPointer<T>> records, BPlusNode<T> nextLeaf) {
        super(pageSize, value);
        this.records = records;
    }

    /**
     * Creates a node in a BPlusTree
     *
     * @param pageSize The page size for the BPlusTree (i.e. the space for
     *                 storing records within a node)
     * @param value The value of this node, used to determine which leaf node
     *              to insert a record into
     */
    public BPlusLeafNode(int pageSize, T value) {
        super(pageSize, value);
    }

    /**
     * Inserts a node into the node. If now oversize, splits node
     * @param rp The RecordPointer being inserted
     * @return `true` if the record was inserted. `false` if a record with
     * that key value already exists in this node
     */
    @Override
    public boolean insertRecord(RecordPointer<T> rp) {
        int insertIndex = 0;
        // Find the index where the record should be inserted, i.e. the index of the first
        // record with a greater value. If no record is larger, the loop will exit with
        // insertIndex == records.size() and the record will get appended
        while (insertIndex < records.size()) {
            int cmp = records.get(insertIndex).compareTo(rp);
            if (cmp > 0) {
                break; // Found larger record
            } else if (cmp == 0) {
                return false;  // Found duplicate
            }
            insertIndex += 1;
        }
        records.add(insertIndex, rp);
        //TODO: Split page if overfull
        //TODO: Update value/parent?
        return true;
    }

    @Override
    public boolean deleteRecord(T value) {
        int deleteIndex = 0;
        // Find the index of the record to delete. If the record was not found,
        // deleteIndex == records.size()
        while (deleteIndex < records.size()) {
            int cmp = records.get(deleteIndex).getValue().compareTo(value);
            if (cmp == 0) {
                break; // Found record
            } else if (cmp > 0) {
                return false;  // Found larger record; target record does not exist
            }
            deleteIndex += 1;
        }
        records.remove(deleteIndex);
        //TODO: Merge leaves if underfull
        //TODO: Update value/parent?
        return true;
    }

    @Override
    public boolean contains(T value) {
        // Searches through all RecordPointers until it finds the value. If it finds
        // a larger value or the loop exists, a matching record does not exist
        for (RecordPointer<T> rp : records) {
            int cmp = rp.getValue().compareTo(value);
            if (cmp > 0) {
                return false; // Found larger record without finding match
            } else if (cmp == 0) {
                return true;  // Found record
            }
        }
        return false;
    }

    @Override
    public RecordPointer<T> get(T value) {
        // Searches through all RecordPointers until it finds the value. If it finds
        // a larger value or the loop exists, a matching record does not exist
        for (RecordPointer<T> rp : records) {
            int cmp = rp.getValue().compareTo(value);
            if (cmp > 0) {
                return null; // Found larger record without finding match
            } else if (cmp == 0) {
                return rp;  // Found record
            }
        }
        return null;
    }
}
