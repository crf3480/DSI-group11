package bplus;

import exceptions.CustomExceptions;
import tableData.Attribute;
import tableData.Bufferable;
import tableData.TableSchema;

import java.io.*;
import java.util.ArrayList;

public class BPlusNode<T extends Comparable<T>> extends Bufferable {
    private final TableSchema schema;
    private ArrayList<BPlusPointer<T>> pointers;
    private Integer parentIndex;
    public int n;

    /**
     * Creates a BPlusNode for a given table
     * @param schema The TableSchema of the table being indexed
     * @param nodeIndex The index of this node in the BPlus file
     * @param pointers The pointers stored in this node
     */
    public BPlusNode(TableSchema schema, int nodeIndex, ArrayList<BPlusPointer<T>> pointers, Integer parentIndex) {
        this.schema = schema;
        this.index = nodeIndex;
        this.pointers = pointers;
        this.parentIndex = parentIndex;
        Attribute pk = schema.attributes.get(schema.primaryKey);
        n = (schema.pageSize / (pk.length + Integer.BYTES + Integer.BYTES)) - 1;
        n = 5;  //TODO: TEST VALUE. REMOVE LATER
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
        return pointers.getFirst().isRecordPointer();
    }

    /**
     * Checks if this node is the root node
     * @return boolean corresponding to if the node is the root or not
     */
    public boolean isRootNode() {
        return parentIndex == null;
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
        // Searches through all pointers until it finds the value. If it finds
        // a larger value or the loop exits, a matching record does not exist
        for (BPlusPointer<T> bpp : pointers) {
            // Last pointer has a null value, meaning you did not find a match
            // Leaf nodes return `null` since there was no match
            // Internal nodes return the pointer to follow
            if (bpp.value == null) {
                return (isLeafNode()) ? null : bpp;
            }
            int cmp = bpp.getValue().compareTo(value);
            if (bpp.isRecordPointer() && cmp > 0) {
                return null; // Found larger record without finding match in leaf node
            } else if (cmp >= 0) {
                return bpp;  // Found matching branch
            }
        }
        // It shouldn't be possible to exit the for-loop
        throw new InternalError("Escaped pointer iterator in get() while looking for `" +
                obj + "` in table `" + schema.name + "` with node `" + index + "`");
    }

    /**
     * Inserts a pointer into the node. If now oversize, splits node
     * @param bpp The BPlusPointer being inserted
     * @return `true` if the record was inserted. `false` if a record with
     * that key value already exists in this node
     */
    public boolean insertRecord(BPlusPointer<T> bpp) {
        // Find the index where the record should be inserted, i.e. the index of the first
        // record with a greater value. If no record is larger, the loop will exit with
        // insertIndex == records.size() and the record will get appended
        int insertIndex = 0;
        while (insertIndex < pointers.size()) {
            int cmp = pointers.get(insertIndex).compareTo(bpp);
            if (cmp > 0) {
                break; // Found larger record
            } else if (cmp == 0) {
                return false;  // Found duplicate
            }
            insertIndex += 1;
        }
        pointers.add(insertIndex, bpp);

        //Split if node is now overfull
        if(parentIndex == null && pointers.size()>n) {   // Root splitting - middle value is new root node's only value
            System.out.println("Splitting time!");
            System.out.println("BEFORE: "+pointers);
            int splitIndex = (pointers.size())/2;
            ArrayList<BPlusPointer<T>> leftSide = new ArrayList<>();
            ArrayList<BPlusPointer<T>> middle = new ArrayList<>();
            ArrayList<BPlusPointer<T>> rightSide = new ArrayList<>();
            leftSide.addAll(pointers.subList(0, splitIndex));
            middle.add(pointers.get(splitIndex));
            rightSide.addAll(pointers.subList(splitIndex+1, pointers.size()));
            System.out.println(leftSide.toString());
            System.out.println(middle.toString());
            System.out.println(rightSide.toString());

            //Make new root
            pointers.clear();
            pointers.addAll(middle);

            //BPlusNode<T> left = new BPlusNode<>(schema, , pointers, parent);




        }

        if(isLeafNode() && pointers.size() >= n){   // leaf node max size of n-1
            int splitIndex = (pointers.size()+1)/2;    //If odd size, the left one gets the extra element
            ArrayList<BPlusPointer<T>> leftSide = new ArrayList<>();
            ArrayList<BPlusPointer<T>> rightSide = new ArrayList<>();
            leftSide.addAll(pointers.subList(0, splitIndex));
            rightSide.addAll(pointers.subList(splitIndex, pointers.size()));

            throw new InternalError("Time to split!");

            /*
                TODO: Leaf node splitting
                 Get parent node(?) Right side needs to be given to parent node
                 insertRecord(rightSide) on the parent node, that way it'll recursively split upwards
                 set this node's pointers array to leftSize.
             */
        }
        else if (pointers.size() > n) {  // internal node max size of n
            int splitIndex = (pointers.size() + 1) / 2;    //If odd size, the left one gets the extra element
            ArrayList<BPlusPointer<T>> leftSide = new ArrayList<>();
            ArrayList<BPlusPointer<T>> rightSide = new ArrayList<>();
            leftSide.addAll(pointers.subList(0, splitIndex));
            rightSide.addAll(pointers.subList(splitIndex, pointers.size()));

            /*
                More pseudocode yay
                TODO: internal node splitting
                    if root, create new root. value = first on right side, pointers are left and right.sublist(1 to end)
                    if not root, split as usual; send right upwards and and replace current pointers with left side
             */
        }
        return true;
    }

    /**
     * Removes a record from the tree
     * @param obj The value of the RecordPointer to remove
     * @return `true` if the record was removed from the node; `false` if a record
     * with that value does not exist
     */
    public boolean deleteRecord(Object obj) {
        if (!isLeafNode()) {
            throw new InternalError("`deleteRecord` called on internal node. To traverse tree " +
                    "for deletion, call `get()` until a leaf node is returned.");
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
        //TODO: Merge leaves if underfull, update parent
        return true;
    }
/*
    public BPlusNode<T> getNode(int pageNum){
        for(BPlusPointer<T> bpp : pointers){
            if(bpp.pageIndex == pageNum){
                return bpp;
            }
        }
    }

 */

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

    /**
     * Parses a BPlusNode out of an array of bytes
     * @param schema The TableSchema for the table the node belongs to
     * @param nodeIndex The index of the node within the B+ Tree file
     * @param nodeData The array of bytes containing the node's data
     */
    public static BPlusNode<?> parse(TableSchema schema, int nodeIndex, byte[] nodeData, BPlusNode<?> parent) throws IOException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(nodeData);
        DataInputStream in = new DataInputStream(inStream);
        in.readByte();  // Ignore metadata byte
        Attribute pk = schema.getPK();
        int n = (schema.pageSize / (pk.length + Integer.BYTES + Integer.BYTES)) - 1;

        switch (pk.type) {
            case INT:
                ArrayList<BPlusPointer<Integer>> intPointers = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    int value = in.readInt();
                    int pageIndex = in.readInt();
                    if (pageIndex == -1) { break; }  // End of pointers flag
                    int secondPointer = in.readInt();
                    intPointers.add(new BPlusPointer<>(value, pageIndex, secondPointer));
                }
                return new BPlusNode<>(schema, nodeIndex, intPointers, parent.index);
            case DOUBLE:
                ArrayList<BPlusPointer<Double>> doublePointers = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    double value = in.readDouble();
                    int pageIndex = in.readInt();
                    if (pageIndex == -1) { break; }  // End of pointers flag
                    int secondPointer = in.readInt();
                    doublePointers.add(new BPlusPointer<>(value, pageIndex, secondPointer));
                }
                return new BPlusNode<>(schema, nodeIndex, doublePointers, parent.index);
            case VARCHAR, CHAR:
                ArrayList<BPlusPointer<String>> strPointers = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    int pageIndex = in.readInt();
                    if (pageIndex == -1) { break; }  // End of pointers flag
                    int secondPointer = in.readInt();
                    String value = in.readUTF();
                    strPointers.add(new BPlusPointer<>(value, pageIndex, secondPointer));
                }
                return new BPlusNode<>(schema, nodeIndex, strPointers, parent.index);
            case BOOLEAN:
                // Who is going to index on a boolean?????
                ArrayList<BPlusPointer<Boolean>> boolPointers = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    boolean value = in.readBoolean();
                    int pageIndex = in.readInt();
                    if (pageIndex == -1) { break; }  // End of pointers flag
                    int secondPointer = in.readInt();
                    boolPointers.add(new BPlusPointer<>(value, pageIndex, secondPointer));
                }
                return new BPlusNode<>(schema, nodeIndex, boolPointers, parent.index);
        }
        return null;
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
        for (BPlusPointer<T> pointer : pointers) {
            sb.append(pointer.toString()+" ");
        }

        return sb.toString();
    }
}
