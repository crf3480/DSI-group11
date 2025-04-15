package bplus;

import tableData.TableSchema;

import java.io.IOException;
import java.util.ArrayList;

class BPlusLeafNode<T extends Comparable<T>> extends BPlusNode<T> {

    public ArrayList<RecordPointer<T>> records;
    //TODO: This attribute exists, but it is never read/updated
    public BPlusNode<T> nextLeaf;

    /**
     * Creates an empty Leaf node for a B+ tree
     * @param schema The TableSchema of the table this node belongs to
     * @param pointer The pointer to this node in the BPlus file
     */
    public BPlusLeafNode(TableSchema schema, long pointer) {
        super(schema, pointer);
        this.records = new ArrayList<>();
    }

    /**
     * Creates a Leaf node for a B+ tree from a collection of RecordPointers
     * @param schema The TableSchema for the table this BPlus tree indexes into
     * @param pointer The pointer to this node in the BPlus file
     * @param records The list of records stored in this node
     * @param nextLeaf The next leaf in the tree. `null` if this is the final leaf
     */
    public BPlusLeafNode(TableSchema schema, long pointer, ArrayList<RecordPointer<T>> records, BPlusNode<T> nextLeaf) {
        super(schema, pointer);
        this.records = records;
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

    @Override
    public void save() throws IOException {
        //TODO: Serialize leaf nodes
    }
}
