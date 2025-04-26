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
    private int parent;

    /**
     * Creates a BPlusNode for a given table
     * @param schema The TableSchema of the table being indexed
     * @param nodeIndex The index of this node in the BPlus file
     * @param pointers The pointers stored in this node
     */
    public BPlusNode(TableSchema schema, int nodeIndex, ArrayList<BPlusPointer<T>> pointers, int parentIndex) {
        this.schema = schema;
        this.index = nodeIndex;
        this.pointers = new ArrayList<BPlusPointer<T>>();
        for (BPlusPointer<?> pointer : pointers) {
            pointers.add((BPlusPointer<T>) pointer);
        }
        this.parent = parentIndex;
    }

    // i hate generics
    public BPlusNode(TableSchema schema, int nodeIndex, ArrayList<BPlusPointer<?>> pointers, int parentIndex, boolean isThisDumb){
        this.schema = schema;
        this.index = nodeIndex;
        this.pointers = new ArrayList<BPlusPointer<T>>();
        System.out.println(pointers.size());
        for (BPlusPointer<?> pointer : pointers) {
            this.pointers.add((BPlusPointer<T>) pointer);
        }
        this.parent = parentIndex;
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

    public int getParent() {
        return parent;
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
            pointers.add(new BPlusPointer<>(null, -1, -1));
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
                pointers.set(i, newBPP);
            } else if (newBPP != null) {
                pointers.set(i, new BPlusPointer<>(bpp.getValue(), bpp.getPageIndex(), bpp.getRecordIndex() + 1));
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
        if(!(pointers.getLast().getValue() == null)) {
            pointers.add(new BPlusPointer<>(null, -1, -1));
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
        //TODO: Merge leaves if underfull, revalidate tree (IN STORAGEMANAGER)
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
    public static BPlusNode<?> parse(TableSchema schema, int nodeIndex, byte[] nodeData, int parentIndex) throws IOException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(nodeData);
        DataInputStream in = new DataInputStream(inStream);
        in.readByte();  // Ignore metadata byte
        Attribute pk = schema.getPrimaryKey();
        int n = (schema.pageSize / (pk.length + Integer.BYTES + Integer.BYTES)) - 1;

        switch (pk.type) {
            case INT:
                ArrayList<BPlusPointer<Integer>> intPointers = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    int value = in.readInt();
                    int pageIndex = in.readInt();
                    int secondPointer = in.readInt();
                    if (pageIndex == -1) {  // null pointer
                        intPointers.add(new BPlusPointer<>(null, secondPointer, -1));
                        break;
                    }
                    intPointers.add(new BPlusPointer<>(value, pageIndex, secondPointer));
                }
                return new BPlusNode<>(schema, nodeIndex, intPointers, parentIndex);
            case DOUBLE:
                ArrayList<BPlusPointer<Double>> doublePointers = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    double value = in.readDouble();
                    int pageIndex = in.readInt();
                    int secondPointer = in.readInt();
                    if (pageIndex == -1) {  // null pointer
                        doublePointers.add(new BPlusPointer<>(null, secondPointer, -1));
                        break;
                    }
                    doublePointers.add(new BPlusPointer<>(value, pageIndex, secondPointer));
                }
                return new BPlusNode<>(schema, nodeIndex, doublePointers, parentIndex);
            case VARCHAR, CHAR:
                ArrayList<BPlusPointer<String>> strPointers = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    int pageIndex = in.readInt();
                    int secondPointer = in.readInt();
                    if (pageIndex == -1) {  // null pointer
                        strPointers.add(new BPlusPointer<>(null, secondPointer, -1));
                        break;
                    }
                    String value = in.readUTF();
                    strPointers.add(new BPlusPointer<>(value, pageIndex, secondPointer));
                }
                return new BPlusNode<>(schema, nodeIndex, strPointers, parentIndex);
            case BOOLEAN:
                // Who is going to index on a boolean?????
                ArrayList<BPlusPointer<Boolean>> boolPointers = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    boolean value = in.readBoolean();
                    int pageIndex = in.readInt();
                    int secondPointer = in.readInt();
                    if (pageIndex == -1) {  // null pointer
                        boolPointers.add(new BPlusPointer<>(null, secondPointer, -1));
                        break;
                    }
                    boolPointers.add(new BPlusPointer<>(value, pageIndex, secondPointer));
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
        Attribute pk = schema.attributes.get(schema.primaryKey);
        int n = (schema.pageSize / (pk.length + Integer.BYTES)) - 1;
        try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw")) {
            long offset = ((long) index * schema.pageSize);  // Page count + pageIndex offset
            raf.seek(offset);
            // Create output byte array
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bs);
            for (BPlusPointer<T> pointer : pointers) {
                out.write(pointer.encode(schema));
            }
            // If node is not full, add a "stop" flag
            if (pointers.size() < n) {
                out.writeInt(-1);
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

    public void addPointer(Object value, int pageIndex){
        pointers.add(new BPlusPointer<>(value, pageIndex, -1));
    }

    public void clearPointers() {
        pointers.clear();
    }

    /**
     * Helper method for casting an object to this node's type
     * @param obj The object being typecast
     * @return An object of this node's type
     */
    @SuppressWarnings("unchecked")
    private T cast(Object obj) {
        try {
            return (T) obj;
        } catch (ClassCastException cce) {
            throw new CustomExceptions.IncompatibleTypeComparisonException("Incompatible index type `" +
                    obj.getClass() + "` for table `" + schema.name + " (Expected: " +
                    schema.attributes.get(schema.primaryKey).type + ")");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NODE "+index+" {");
        for (BPlusPointer<T> pointer : pointers) {
            sb.append(pointer.getValue()+" "+pointer.getPageIndex()+((!pointer.equals(pointers.getLast()))? ", ":""));
        }
        sb.append("}");

        return sb.toString();
    }
}
