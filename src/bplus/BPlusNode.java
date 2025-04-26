package bplus;

import exceptions.CustomExceptions;
import tableData.Attribute;
import tableData.Bufferable;
import tableData.TableSchema;

import java.io.*;
import java.util.ArrayList;

public class BPlusNode<T extends Comparable<T>> extends Bufferable {

    private final TableSchema schema;
    private final ArrayList<BPlusPointer<T>> pointers;
    public final int parent;
    public final int n;

    /**
     * Creates a BPlusNode for a given table
     * @param schema The TableSchema of the table being indexed
     * @param nodeIndex The index of this node in the BPlus file
     * @param pointers The pointers stored in this node
     * @param parentIndex The index of the node's parent. -1 if this is the root node
     */
    public BPlusNode(TableSchema schema, int nodeIndex, ArrayList<BPlusPointer<T>> pointers, int parentIndex) {
        this.schema = schema;
        this.index = nodeIndex;
        this.pointers = pointers;
        this.parent = parentIndex;
        Attribute pk = schema.getPrimaryKey();
        int availablePageSpace = (schema.pageSize - Integer.BYTES); // Parent pointer takes up 4 bytes
        int bppSize = (pk.length + Integer.BYTES + Integer.BYTES);  // value + page pointer + record pointer
        n = (availablePageSpace / bppSize);
    }

    // i hate generics
    public BPlusNode(TableSchema schema, int nodeIndex, ArrayList<BPlusPointer<?>> pointers, int parentIndex, boolean isThisDumb){
        this.schema = schema;
        this.index = nodeIndex;
        this.pointers = new ArrayList<>();
        System.out.println(pointers.size());
        for (BPlusPointer<?> pointer : pointers) {
            this.pointers.add(castPointer(pointer));
        }
        this.parent = parentIndex;

        Attribute pk = schema.getPrimaryKey();
        int availablePageSpace = (schema.pageSize - Integer.BYTES); // Parent pointer takes up 4 bytes
        int bppSize = (pk.length + Integer.BYTES + Integer.BYTES);  // value + page pointer + record pointer
        n = (availablePageSpace / bppSize);

        if (!isThisDumb) {
            System.err.println("Yes it is");
        }
    }

    /**
     * Returns the number of pointers in the node
     * @return the number of pointers in the node
     */
    public int size() {
        return pointers.size();
    }

    /**
     * Returns the name of the table this BPlusNode indexes into
     * @return The table name
     */
    public String getTableName() {
        return schema.name;
    }

    /**
     * Checks if this BPlusNode is a leaf node rather than an internal node
     * @return `true` if this object is a leaf node; `false` if this object is an internal node
     */
    public boolean isLeafNode() {
        return pointers.isEmpty() || pointers.getFirst().isRecordPointer();
    }

    /**
     * Checks if this node is the root node
     * @return boolean corresponding to if the node is the root or not
     */
    public boolean isRootNode() {
        return parent == -1;
    }

    /**
     * Gets the BPlusPointer for a given value. If this is an internal node, returns
     * the BPlusPointer for the child node whose branch the value should be in
     * @param obj The value whose pointer is being searched for
     * @return The BPlusPointer for that value; `null` if this is a leaf node and
     * no pointer matches the given value
     */
    public BPlusPointer<T> get(Object obj) {
        T value = cast(obj);
        if (pointers.isEmpty()) {
            return null;
        }
        // Searches through all pointers until it finds the value. If it finds
        // a larger value or the loop exists, a matching record does not exist
        for (BPlusPointer<T> bpp : pointers) {
            // Last pointer of internal nodes has a null value, meaning you did not find a match
            // Leaf nodes return `null` since there was no match
            // Internal nodes return the pointer to follow
            if (bpp.getValue() == null) {
                return (isLeafNode()) ? null : bpp;
            }
            int cmp = bpp.getValue().compareTo(value);
            if (bpp.isRecordPointer() && cmp > 0) {
                return null; // Found larger record without finding match in leaf node
            } else if (cmp >= 0) {
                return bpp;  // Found matching branch
            }
        }
        // It shouldn't be possible to exit the for-loop in an internal node
        throw new InternalError("Escaped pointer iterator in get() while looking for `" +
                obj + "` in table `" + schema.name + "` with node `" + index + "`");
    }

    /**
     * Inserts a pointer into the BPlus node. All subsequent pointers from the same page will
     * have their record index incremented
     * @param obj The value being inserted
     * @return The B+ pointer where this record was inserted
     */
    public BPlusPointer<T> insertRecord(Object obj) {
        if (!isLeafNode()) {
            throw new IllegalArgumentException("node is not leaf. failed.");
        }
        T value = cast(obj);
        if (pointers.isEmpty()) {
            BPlusPointer<T> firstRecord = new BPlusPointer<>(value, 0, 0);
            pointers.add(firstRecord);
            pointers.add(new BPlusPointer<>(null, -1));
            return firstRecord;
        }
        // Find the index where the record should be inserted, i.e. the index of the first
        // record with a greater value. If no record is larger, insert at the end
        BPlusPointer<T> newBPP = null;
        for (int i = 0; i < pointers.size(); i++) {
            BPlusPointer<T> bpp = pointers.get(i);
            if (bpp.getValue() == null && newBPP == null) {
                // Insert after last non-null pointer
                BPlusPointer<T> prevPointer = pointers.get(i - 1);
                newBPP = new BPlusPointer<>(value, prevPointer.getPageIndex(), prevPointer.getRecordIndex() + 1);
                pointers.add(i, newBPP);
            } else if (newBPP != null) {
                if (bpp.getValue() != null) {
                    pointers.set(i, new BPlusPointer<>(bpp.getValue(), bpp.getPageIndex(), bpp.getRecordIndex() + 1));
                }
            } else {
                int cmp = bpp.getValue().compareTo(value);
                if (cmp > 0) {
                    // New BPP goes takes the spot of the first pointer with a larger value
                    newBPP = new BPlusPointer<>(value, bpp.getPageIndex(), bpp.getRecordIndex());
                    pointers.add(i, newBPP);
                } else if (cmp == 0) {
                    throw new IllegalArgumentException("Duplicate key: " + value);
                }
            }
        }
        return newBPP;
    }

    /**
     * Removes a record from the tree
     * @param obj The value of the RecordPointer to remove
     * @return `true` if the record was removed from the node; `false` if a record
     * with that value does not exist
     */
    public boolean deleteRecord(Object obj) {
        if (!isLeafNode()) {
            return false;
        }

        T value = cast(obj);
        int deleteIndex = 0;
        // Find the index of the record to delete. If the record was not found,
        // deleteIndex == records.size()
        while (deleteIndex < pointers.size()) {
            int cmp = pointers.get(deleteIndex).getValue().compareTo(value);
            if (cmp == 0) {
                break; // Found record
            } else if (cmp > 0) {
                return false;  // Found larger record; target record does not exist
            }
            deleteIndex += 1;
        }
        pointers.remove(deleteIndex);
        return true;
    }

    public ArrayList<BPlusPointer<T>> getPointers() {
        return pointers;
    }

    /**
     * Parses a BPlusNode out of an array of bytes
     * @param schema The TableSchema for the table the node belongs to
     * @param nodeIndex The index of the node within the B+ Tree file
     * @param nodeData The array of bytes containing the node's data
     */
    public static BPlusNode<?> parse(TableSchema schema, int nodeIndex, byte[] nodeData) throws IOException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(nodeData);
        DataInputStream in = new DataInputStream(inStream);
        int parentIndex = in.readInt();
        Attribute pk = schema.getPrimaryKey();
        System.out.println("Reading node index " + nodeIndex);
        int pageIndex = 0;
        int recordIndex = 0;
        switch (pk.type) {
            case INT:
                ArrayList<BPlusPointer<Integer>> intPointers = new ArrayList<>();
                while (pageIndex >= 0) {
                    pageIndex = in.readInt();
                    recordIndex = in.readInt();
                    if (pageIndex == -1) {  // null pointer
                        intPointers.add(new BPlusPointer<>(null, recordIndex));
                        break;
                    }
                    int value = in.readInt();
                    intPointers.add(new BPlusPointer<>(value, pageIndex, recordIndex));
                }
                return new BPlusNode<>(schema, nodeIndex, intPointers, parentIndex);
            case DOUBLE:
                ArrayList<BPlusPointer<Double>> doublePointers = new ArrayList<>();
                while (pageIndex >= 0) {
                    pageIndex = in.readInt();
                    recordIndex = in.readInt();
                    if (pageIndex == -1) {  // null pointer
                        doublePointers.add(new BPlusPointer<>(null, recordIndex));
                        break;
                    }
                    double value = in.readDouble();
                    doublePointers.add(new BPlusPointer<>(value, pageIndex, recordIndex));
                }
                return new BPlusNode<>(schema, nodeIndex, doublePointers, parentIndex);
            case VARCHAR, CHAR:
                ArrayList<BPlusPointer<String>> strPointers = new ArrayList<>();
                while (pageIndex >= 0) {
                    pageIndex = in.readInt();
                    recordIndex = in.readInt();
                    if (pageIndex == -1) {  // null pointer
                        strPointers.add(new BPlusPointer<>(null, recordIndex));
                        break;
                    }
                    String value = in.readUTF();
                    strPointers.add(new BPlusPointer<>(value, pageIndex, recordIndex));
                }
                return new BPlusNode<>(schema, nodeIndex, strPointers, parentIndex);
            case BOOLEAN:
                // Who is going to index on a boolean?????
                ArrayList<BPlusPointer<Boolean>> boolPointers = new ArrayList<>();
                while (pageIndex >= 0) {
                    pageIndex = in.readInt();
                    recordIndex = in.readInt();
                    if (pageIndex == -1) {  // null pointer
                        boolPointers.add(new BPlusPointer<>(null, recordIndex));
                        break;
                    }
                    boolean value = in.readBoolean();
                    boolPointers.add(new BPlusPointer<>(value, pageIndex, recordIndex));
                }
                return new BPlusNode<>(schema, nodeIndex, boolPointers, parentIndex);
        }
        return null;
    }

    @Override
    public void save() throws IOException {
        // Verify table exists
        File indexFile = schema.indexFile();
        if (!indexFile.exists()) {
            throw new IOException("Could not find index file `" + indexFile.getAbsolutePath() + "`");
        }
        // Write data
        try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw")) {
            long offset = ((long) index * schema.pageSize);  // Page count + pageIndex offset
            raf.seek(offset);
            // Create output byte array
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bs);
            out.writeInt(parent);
            for (BPlusPointer<T> pointer : pointers) {
                out.write(pointer.encode(schema));
            }
            byte[] pageData = bs.toByteArray();
            if (pageData.length > schema.pageSize) {
                System.err.println("Node data array exceeded pageSize while saving");
            }
            // Write output
            raf.write(pageData);
        } catch (IOException ioe) {
            throw new IOException("Encountered problem while attempting to write to index file: " + ioe.getMessage());
        }
    }

    /**
     * Adds a pointer to this internal node which is the result of one of its children splitting
     * @param rightObj The value of the first pointer in the right node of the split
     * @param rightIndex The index of the right node of the split
     */
    public void splitPointer(Object rightObj, int rightIndex) {
        T rightValue = cast(rightObj);
        for (int i = 0; i < pointers.size(); i++) {
            BPlusPointer<T> bpp = pointers.get(i);
            // if bpp's value is null or greater than rightValue, that's the ptr that is splitting
            if (bpp.getValue() == null || bpp.getValue().compareTo(rightValue) > 0) {
                BPlusPointer<T> newBPP = new BPlusPointer<>(bpp.getValue(), rightIndex);
                pointers.set(i, new BPlusPointer<>(rightValue, bpp.getPageIndex()));
                pointers.add(i + 1, newBPP);
                return;
            }
        }
        throw new InternalError("Escaped for-loop in splitPointer() while adding value `" +
                rightValue + "` and index `" + rightIndex + "` to " + this);
    }

    /**
     * Inserts a BPlusPointer into this node
     * @param newPtr The pointer being inserted
     */
    public void insertPointer(BPlusPointer<?> newPtr) {
        BPlusPointer<T> bpp = castPointer(newPtr);
        // If adding a null pointer, stick it in and return
        if (bpp.getValue() == null) {
            if (pointers.getLast().getValue() == null) {
                throw new IllegalArgumentException("BPlusNodes cannot have two null pointers");
            }
            pointers.add(bpp);
            return;
        }
        // Iterate through the list of pointers to find where it should go and insert it
        for (int i = 0; i < pointers.size(); i++) {
            BPlusPointer<T> currPtr = pointers.get(i);
            // Since the new ptr isn't null (that was already checked for), it must go before the null ptr
            if (currPtr.getValue() == null || currPtr.getValue().compareTo(bpp.getValue()) > 0) {
                pointers.add(i, bpp);
                return;
            }
        }
        throw new InternalError("Escaped for-loop in insertPointer() while inserting " +
                newPtr + " into " + this);
    }

    /**
     * Replace's this node's pointer list with a new list
     * @param newPointers The list of new pointers
     */
    public void replacePointers(ArrayList<BPlusPointer<?>> newPointers) {
        pointers.clear();
        for (BPlusPointer<?> bpp : newPointers) {
            pointers.add(castPointer(bpp));
        }
    }

    /**
     * Updates all the BPlusPointers for records which were transferred to a new page
     * following a page split
     * @param splitObj The key value of the first record on the new page
     * @param parentIndex The index of the parent page that was split
     * @param splitIndex The index of the new page
     * @param startingRecIndex The index of the first record to check
     * @return The record index for the last pointer updated. The update needs to continue on
     * the next node starting at that value + 1; -1 if the end of the parent page was reached
     * and the update is complete
     */
    public int pageSplit(Object splitObj, int parentIndex, int splitIndex, int startingRecIndex) {
        T splitValue = cast(splitObj);
        int recIndex = startingRecIndex;
        // Iterate through the records until you find a value at (or after) the split point
        for (int i = 0; i < pointers.size() - 1; i++) {
            BPlusPointer<T> bpp = pointers.get(i);
            if (bpp.getValue().compareTo(splitValue) >= 0) {
                // Once you've found the split point, loop over the remaining records and replace
                // their pointers with ones to the new page, resetting their recordPointers to
                // start from startingRecIndex
                while (bpp.getValue() != null) {
                    if (bpp.getPageIndex() != parentIndex) {
                        return -1;  // Reached the end of the page
                    }
                    pointers.set(i, new BPlusPointer<>(bpp.getValue(), splitIndex, recIndex));
                    recIndex += 1;
                    i += 1;
                    bpp = pointers.get(i);
                }
                return recIndex;  // Reached the end of the pointer list without finding the end of the page
            }
        }
        // If you didn't find the split point, pageSplit() was somehow called on the wrong node
        throw new IllegalArgumentException("Failed to locate valid record while performing pageSplit(). " +
                "Obj: " + splitObj + ", pI: " + parentIndex + ", sI: " + splitIndex + ", node: " + this);
    }

    /**
     * Helper method for casting an object to this node's type
     * @param obj The object being typecast
     * @return An object of this node's type; `null` if obj was null
     */
    @SuppressWarnings("unchecked")
    private T cast(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return (T) obj;
        } catch (ClassCastException cce) {
            throw new CustomExceptions.IncompatibleTypeComparisonException("Incompatible index type `" +
                    obj.getClass() + "` for table `" + schema.name + " (Expected: " +
                    schema.attributes.get(schema.primaryKey).type + ")");
        }
    }

    /**
     * Casts a B+ pointer to this node's generic type
     * @param pointer The pointer being cast
     * @return A pointer of this node's type with the same indices as the passed in pointer
     */
    private BPlusPointer<T> castPointer(BPlusPointer<?> pointer) {
        return new BPlusPointer<>(cast(pointer.getValue()), pointer.getPageIndex(), pointer.getRecordIndex());
    }

    @Override
    public String toString() {
        return "(NODE " + index + " <" + parent + "> " + pointers + ")";
    }
}
