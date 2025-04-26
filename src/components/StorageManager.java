package components;
import bplus.*;
import exceptions.CustomExceptions.*;
import tableData.*;
import tableData.Record;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * StorageManager.java
 * Manages fetching and saving pages to file
 */
public class StorageManager {
    private boolean NUKE_MODE = false;
    private final Buffer buffer;
    Catalog catalog;
    int nextTempID;

    /**
     * Creates a StorageManager object
     * @param databaseDir A File object pointing to the directory where the database files are stored
     * @param pageSize The page size used. If a catalog already exists, the page size of that catalog
     *                 will be used instead
     * @param bufferSize The size of the page buffer
     * @param indexing `true` if indexing is turned on; `false` otherwise
     * @throws IOException If there are problems accessing or modifying the catalog and table files
     */
    public StorageManager(File databaseDir, int pageSize, int bufferSize, boolean indexing) throws IOException {
        File catalogFile = new File(databaseDir, "catalog.bin");
        catalog = new Catalog(catalogFile, pageSize, indexing);
        buffer = new Buffer(catalog, bufferSize, catalog.pageSize());
        wipeTempTables();
        nextTempID = 0;
    }

    /**
     * Gets the status of indexing
     * @return `true` if PK indices are enabled
     */
    public boolean isIndexingEnabled() {
        return catalog.indexingEnabled();
    }

    /**
     * Returns a name for a temporary table
     * @return The name of a temporary table
     */
    public String getTempTableName() {
        return String.valueOf(nextTempID++);
    }

    /**
     * Returns a specific Page from a table, addressed by its index
     * @param schema The TableSchema of the table the Page belongs to
     * @param pageIndex The number of the Page being fetched
     * @return The fetched Page; `null` if no page exists with that number
     */
    public Page getPageByIndex(TableSchema schema, int pageIndex) {
        return buffer.getPage(schema, pageIndex);
    }

    /**
     * Returns a specific Page from a table, addressed by its page number
     * @param schema The TableSchema of the table the Page belongs to
     * @param pageNumber The index of the Page being fetched
     * @return The fetched Page; `null` if no page exists with that index
     */
    public Page getPage(TableSchema schema, int pageNumber) {
        int pageIndex = schema.getIndex(pageNumber);
        return buffer.getPage(schema, pageIndex);
    }

    //TODO: This function doesn't need to exist
    public BPlusNode getNode(TableSchema schema, int pageIndex, int parentIndex) {
        BPlusNode out = buffer.getNode(schema, pageIndex, parentIndex);
        return out;
    }

    /**
     * Fetches the BPlusPointer for the record with a given value
     * @param schema The TableSchema of the table being searched
     * @param value The value to find the RecordPointer for
     * @return The pointer for the value; `null` if value does not exist in the table
     */
    private BPlusPointer<?> getIndex(TableSchema schema, Object value) {
        BPlusNode<?> node = buffer.getNode(schema, schema.treeRoot, -1);
        while (node != null) {
            BPlusPointer<?> pointer = node.get(value);
            if (node.isLeafNode()) {
                return pointer;
            }
            // If pointer was a node pointer, follow it
            node = getNode(schema, pointer.getPageIndex(), node.index);
        }
        return null;
    }

    /**
     * Fetches the BPlusPointer for where a primary key value should be inserted into a table
     * @param schema The TableSchema of the table being searched
     * @param value The primary key value to find the insertion point for
     * @return The pointer for where a record would be inserted into a table; `null` if a
     * matching record already exist in the table
     */
    private BPlusPointer<?> getInsertIndex(TableSchema schema, Object value) {
        BPlusNode<?> node = buffer.getNode(schema, schema.treeRoot, -1);
        while (node != null) {
            BPlusPointer<?> pointer = node.get(value);
            if (getNode(schema, pointer.getPageIndex(), pointer.getRecordIndex()).get(value).isRecordPointer()) {
                return pointer;
            }
            // If pointer was a node pointer, follow it
            node = buffer.getNode(schema, pointer.getPageIndex(), node.index);
        }
        return null;
    }

    /**
     * Inserts a record into the table, ordered by the specified attribute
     * @param schema The TableSchema of the table the record is being inserted into
     * @param record The record to insert
     * @param attrIndex The index of the attribute the table will be sorted by
     * @return `true` if the record was successfully inserted; `false` otherwise
     */
    public boolean insertRecord(TableSchema schema, Record record, int attrIndex){
        try{
            insertRecordTry(schema, record, attrIndex);
        } catch (InternalError | IOException e) {
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }

    public boolean insertRecordTry(TableSchema schema, Record record, int attrIndex) throws IOException {
        // Generate the BPlusPointer for where the record needs to be inserted
        Object value = record.get(attrIndex);

        // If table has no pages, make a new page and insert it into the buffer
        if (schema.rootIndex == -1) {
            Page firstPage = new Page(0, 0, schema);
            try {
                firstPage.save();
                buffer.insert(firstPage);
            } catch (IOException ioe) {
                System.err.println("Encountered exception while adding new page to table file: " + ioe.getMessage());
                return false;
            }
            try {
                if (isIndexingEnabled()) {
                    schema.treeRoot=addPage(schema.indexFile());
                    BPlusNode<?> root = new BPlusNode<>(schema, schema.rootIndex, new ArrayList<>(), -1);
                    root.save();
                    System.out.println("Created root "+root);
                    buffer.insert(root);
                    System.out.println("Created "+schema.indexFile().getPath()+" with n of "+String.valueOf((schema.pageSize / (schema.getPrimaryKey().length + (2 * Integer.BYTES))) - 1));
                }
            } catch (IOException ioe) {
                System.err.println("Encountered exception while adding new node to b+ tree: " + ioe.getMessage());
            }
        }
        int targetPageIndex = -1;
        int targetRecordIndex = -1;
        if(!isIndexingEnabled()) {
            // Verify record is unique. While looping, find and remember the insertion point
            int pageNum = 0;
            Page currPage = getPage(schema, pageNum);
            while (currPage != null) {
                for (int i = 0; i < currPage.recordCount(); i++) {
                    Record existingRec = currPage.records.get(i);
                    // Check for duplicate
                    int matchAttr = record.isEquivalent(existingRec, schema);
                    if (matchAttr != -1) {
                        System.err.println("Invalid new tuple (" + record + "): the value '" + record.get(matchAttr) +
                                "' already exists in " + ((schema.attributes.get(matchAttr).primaryKey ? "primary key " : "unique ")
                                + "column '" + schema.attributes.get(matchAttr).name + "'."));
                        return false;
                    }
                    // Check for insertion point
                    if (targetPageIndex == -1 && !record.greaterThan(existingRec, schema, attrIndex)) {
                        targetPageIndex = currPage.index;
                        targetRecordIndex = i;
                    }
                }
                pageNum += 1;
                currPage = getPage(schema, pageNum);
            }
            // If targetPageNum was never updated, record goes at the end of the table
            if (targetPageIndex == -1) {
                targetPageIndex = schema.getIndex(schema.pageCount() - 1);
            }
        }
        else{   //Indexing enabled. Do B+ tree stuff
            System.out.println("\nInserting "+value+" into B+ tree");
            BPlusNode<?> root = buffer.getNode(schema, schema.treeRoot, -1);

            // Traverse tree until you find leaf node where the record will be inserted
            BPlusPointer<?> bpp = root.get(value);
            BPlusNode<?> targetNode = root;
            while (bpp != null) {
                if (targetNode.isLeafNode()) {
                    throw new IllegalArgumentException("Duplicate record: " + record);
                }
                targetNode = buffer.getNode(schema, bpp.getPageIndex(), targetNode.index);
                bpp = targetNode.get(value);
            }

            // Insert the record into the node
            BPlusPointer<?> insertPointer = targetNode.insertRecord(value);
            targetPageIndex = insertPointer.getPageIndex();
            targetRecordIndex = insertPointer.getRecordIndex();

            /*
                n (the big parenthetical representing the max number of pointers a node can have) is calculated as follows:
                page size / (primary key size + page pointer size + record pointer size)
                    - page pointer is an index that refers to the page (or node) number in the table (or b+ tree)
                    - record pointer is an index that refers to the index of the record in the page of the table (or -1 in an internal node)
             */
            displayTree(schema, getNode(schema, schema.rootIndex, -1), "");
            int n = (schema.pageSize / (schema.getPrimaryKey().length + (2 * Integer.BYTES))) - 1;
            n = 6; //TODO: TEST VALUE, DELETE LATER
            if(!isValid(schema, root, n)){
                System.out.println("TREE INVALID, FIXING...");
                validate(schema, root, n);
            }
        }
        // Insert record into target page/index
        Page targetPage = getPageByIndex(schema, targetPageIndex);

        if (targetRecordIndex == -1) {
            targetPage.records.add(record);

        } else {
            targetPage.records.add(targetRecordIndex, record);
        }
        schema.incrementRecordCount();

        // If the page is now oversize, split
        if (targetPage.pageDataSize() > catalog.pageSize()) {
            int childIndex = schema.getFirstPageGap();
            if (childIndex == -1) {
                try {
                    childIndex = addPage(schema.tableFile());
                } catch (IOException e) {
                    // If there was a failure, undo the record insert and abort
                    System.err.println(e.getMessage());
                    targetPage.records.remove(targetRecordIndex);
                    return false;
                }
            }
            Page child = targetPage.split(childIndex);
            // Insert the new page into the buffer and catalog
            try {
                System.out.println("Inserting child " + child.pageNumber + " at " + childIndex);
                buffer.insert(child);
                schema.insertPage(child.pageNumber, childIndex);
                child.save();
            } catch (IOException ioe) {
                System.err.println("Failed to write split page to file. Error: " + ioe.getMessage());
            }
            buffer.refreshPageNumbers(schema); // Resync pageNumber for pages in buffer
        }
        return true;
    }

    /**
     * Validate a given B+ Tree, performing splits on overfull nodes.
     * @param root the root of the tree
     */
    private void validate(TableSchema schema, BPlusNode<?> root, int n) {
        // We validate the tree bottom up to avoid needing to call this more than once, so we recurse down first
        if(!root.isLeafNode()) {
            for (int i = 0; i < root.getPointers().size(); i++) {
                BPlusPointer<?> bpp = root.getPointers().get(i);
                validate(schema, buffer.getNode(schema, bpp.getPageIndex(), root.index), n);
            }
        }
        root = buffer.getNode(schema, root.index, root.getParent());
        if(root.size() > n) {
            /*
                Node splitting
                ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⠤⠐⠒⢀⠋⡉⢍⢫⡝⣫⢟⡶⣲⢦⣠⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡀⠔⠈⠁⠀⡀⠐⡈⠀⠄⠱⣈⠦⡹⣜⡹⣞⡽⣯⣟⣯⣷⣷⣦⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠔⠨⣀⠀⠀⢀⠡⠀⡁⠄⡈⠄⢁⠢⡑⡱⢌⡳⣝⡞⣷⢯⡿⣽⣿⣿⣿⣷⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡠⠃⠀⠀⠀⠛⢿⣤⣀⡄⠀⠠⠀⠠⢀⠀⢣⡘⢇⠧⣜⣻⡼⣟⡿⣿⣻⣿⣿⣿⣿⣿⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠊⠀⠀⠀⠀⠀⠀⠀⡈⣙⠿⣷⣦⠁⠂⠄⡈⠐⡌⢎⡜⣎⢷⣹⣯⣿⣿⢿⣿⣿⣿⣿⣿⣿⣷⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⠀⡐⠁⡐⢁⠀⠒⠈⠉⠉⠉⠐⠢⣍⡢⡀⠄⠡⠐⢀⡑⢌⠲⣜⢺⣿⢟⣫⠷⠋⠉⠀⠀⠉⠙⠛⢿⣿⣿⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⡐⢀⢘⠔⠁⠀⠀⠀⠀⡠⢴⣶⣦⡈⠻⣌⢆⠐⠈⠠⡐⠬⡱⢌⣧⣫⡾⢁⠐⢲⣶⣶⣄⠀⠀⠀⠀⠙⣿⣿⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠰⢀⢂⣎⠠⠤⠀⠀⠀⠤⠓⠾⠿⢿⡷⠀⠙⣎⠀⡁⢂⡑⠦⡱⢭⣳⠏⠀⢸⣶⣿⢿⠿⠿⣤⣤⣤⣤⣤⣸⣿⣿⣆⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⢀⢇⡉⠩⡀⢔⡠⢢⣌⡤⣬⢍⣥⣖⣢⣬⣕⡢⢜⣇⡀⢣⠘⡴⡙⣾⣏⣤⣖⣯⣵⣲⣯⣭⣿⣽⣿⣿⣿⣿⣿⣿⣿⣿⡀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⢸⢎⡰⣁⠚⠤⣉⠧⢘⠰⠃⠚⠄⠫⠜⠣⢛⠹⠓⣌⠲⣡⢋⡴⡹⣖⢯⡟⣿⢿⡿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⣞⣎⢖⣡⢋⡴⡀⠆⡄⢂⠌⡐⡈⠄⣂⠡⢂⠥⣩⠤⣗⣒⣛⣖⣻⡼⣧⢿⣹⡾⣽⡷⣿⣯⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⣿⡼⣚⢦⣏⢴⣉⠞⣌⠲⡌⠴⣁⠎⡴⣉⢎⣼⡵⡿⠉⠈⠙⡟⠉⠙⢯⣻⣷⣻⡽⣟⣷⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⢻⡷⣯⢷⢮⣳⢮⡝⢦⠳⣜⢣⣜⢮⣱⠮⡯⠃⠀⠇⠀⠀⠀⠂⠀⠀  ⠈⣷⣯⢿⣿⣽⣿⢿⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⢸⣿⣽⣯⣟⣮⢷⣫⣏⠿⣜⢧⣯⡿⣜⣾⣷⣶⣶⣶⣿⣿⣿⣿⣿⣿⣿⣿⣿⣟⣿⣯⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⢿⣿⣾⣟⣾⣯⢷⣯⢿⡽⣾⣾⣷⣽⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⢼⣿⣻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠘⣿⣿⣿⣿⣾⣿⢾⣯⣟⣧⣿⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠞⣿⣿⣿⣿⣿⣿⣿⣿⣿⣻⣿⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⠹⣿⣿⣿⣿⣿⣿⣿⣾⣿⡏⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡏⣿⣿⣿⣿⣿⣿⢿⣳⣿⣻⣽⠎⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⠀⠘⢿⣿⣿⣿⣿⣿⣿⣿⡧⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⣿⣿⣿⣿⣻⢾⣻⡽⢾⡽⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠻⣿⣿⣿⣿⣿⣿⣟⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣟⡾⡝⣏⢶⡹⠗⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⡎⠁⠀⠁⠒⢰⣶⠒⠂⠙⠛⢻⣿⣿⣿⣯⡟⢻⠛⠛⣿⣿⣿⣿⣿⣿⣿⠛⠛⡟⠛⠛⠉⣿⣿⣿⠘⣧⣽⡜⣾⠉⣬⣭⠓⣤⠀⠀⠀⠀⠀⠀⠀⠀⠀
                ⠀⠀⢇⠀⠀⠀⠀⠀⢈⣀⠀⠀⠀⠀⠈⠙⢿⣿⣿⣜⡀⡀⡄⠀⠀⢀⠀⠀⢠⠀⠀⣁⠀⢀⣶⡇⠿⣸⣱⣾⡶⠛⠉⠁⠀⠀⠈⠉⠓⠂⠒⠀⠈⠀⠢⠀⠀
                ⠀⠀⠈⠂⢄⣀⡀⠀⢾⣿⣟⡦⢤⣀⣀⢀⢤⣹⣿⣿⣿⣷⣷⣶⣶⣿⣶⣴⣿⣶⡶⠟⠛⢋⣡⣴⣿⠿⣻⢣⡀⠀⠀⡀⡠⢤⣴⣶⡄⠀⠀⠀⠀⠀⡜⠀⠀
                ⠀⠀⠀   ⠝⠪⠧⣤⠀⢈⠿⠒⠬⣎⣢⠭⢷⠫⠟⡷⢿⣭⣯⣵⣉⣌⣡⣤⣴⣶⢿⠿⠻⠝⠢⢹⠯⣅⡫⣍⠧⢽⠗⠓⠛⢋⢵⢤⣤⣤⣴⡞⠁⠀⠀
                ⠀⠀   ⡧⡀⠀⠀⠀⡹⠋⢀⣤⢤⣿⡀⠀⠈⠆⠀⠀⠀⠀⠀⠀⠉⠈⠁⠀⠀⠀⠀⠀⠀⠀⠠⡁⠀⠀⣿⡇⣀⠀⠱⡠⠚⠋⠀⠈⠀⠀
                ⠀     ⠈⠐⠒⢲⠁⠀⠀⠈⠙⣿⠁⠉⠉⢺⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀ ⠉⠁⢸⣿⠟⠋⠁⠀⠑⣀⠀⣀⠠⠞
                     ⠀⠀⠀⠀⠀⠓⠢⠤⠤⠴⠃⠣⢀⠠⠞⠀⠀                 ⠣⠄⣀⠝⠘⠄⣀⣀⣀⠴⠋⠀


                i hate generics i hate generics i hate generics i hate generics i hate generics i hate generics
i hate generics i hate generics i hate generics i hate generics i hate generics i hate generics i hate generics
i hate generics i hate generics i hate generics i hate generics i hate generics i hate generics i hate generics
i hate generics i hate generics i hate generics i hate generics i hate generics i hate generics i hate generics
             */
            int splitIndex = (root.size())/2;
            System.out.println(root.size()+" splits at index "+splitIndex);
            ArrayList<BPlusPointer<?>> leftSide = new ArrayList<>();
            ArrayList<BPlusPointer<?>> middle = new ArrayList<>();
            ArrayList<BPlusPointer<?>> rightSide = new ArrayList<>();

            try{
                File file = schema.indexFile();

                /*
                    If we're splitting the root node, create a new root (this implementation keeps the root the same,
                    instead option to create two new child nodes, since reassigning the root node is much more of a
                    hassle than necessary).

                    The new root has a single value in it (the middle one)
                    and the remaining values are distributed evenly between them (left getting the +1 if it's odd)
                 */
                ArrayList<? extends BPlusPointer<?>> pointers = root.getPointers();
                if(root.isRootNode()){
                    System.out.println("Splitting root node "+root);
                    leftSide.addAll(pointers.subList(0, splitIndex));
                    middle.add(pointers.get(splitIndex));
                    rightSide.addAll(pointers.subList(splitIndex, pointers.size()));

                    root.clearPointers();
                    int leftIndex = addPage(file);
                    int rightIndex = addPage(file);
                    leftSide.add(new BPlusPointer<>(null, rightIndex, -1));
                    //System.out.println("Left index: " + leftIndex);
                    //System.out.println("Right index: " + rightIndex);
                    root.addPointer(middle.getFirst().getValue(), leftIndex);
                    System.out.println(root.getPointers());

                    /*
                        Inner nodes have a null value pointer at the end to represent the "greater than all" case.
                        That way we don't need to save all the pointers and values separately.
                     */
                    root.addPointer(null, rightIndex);
                    root.save();

                    BPlusNode<?> leftNode = new BPlusNode<>(schema, leftIndex, leftSide, root.index, true);
                    leftNode.save();

                    BPlusNode<?> rightNode = new BPlusNode<>(schema, rightIndex, rightSide, root.index, true);
                    rightNode.save();
                }
                else {
                    System.out.println("Splitting internal node "+root);
                    int rightIndex = addPage(file)+1;
                    leftSide.addAll(pointers.subList(0, splitIndex));
                    leftSide.add(new BPlusPointer<>(null, rightIndex, -1));
                    rightSide.addAll(pointers.subList(splitIndex, pointers.size()));
                    System.out.println("left side:  "+leftSide.toString());
                    System.out.println("right side: "+rightSide.toString());

                    root.clearPointers();
                    for (BPlusPointer bpp : leftSide) {
                        root.addPointer(bpp.getPageIndex(), bpp.getRecordIndex());
                    }

                    BPlusNode<?> parent = getNode(schema, root.getParent(), root.index);
                    parent.addPointer(rightSide.getFirst().getValue(), rightIndex);
                    parent.save();

                    BPlusNode<?> rightNode = new BPlusNode<>(schema, rightIndex, rightSide, root.index, true);
                    rightNode.save();
                }
            } catch (IOException ioe){
                System.err.println(ioe.getMessage());
            }
        }
        displayTree(schema, root, "");
    }

    /**
     * Determine whether the subtree headed by root is a valid B+ Tree
     * @param schema The TableSchema the B+ Tree is indexing on
     * @param root  The root of the subtree
     * @return True if all contained nodes are valid, else false
     */
    private boolean isValid(TableSchema schema, BPlusNode<?> root, int n) {
        if(root.size() > n){
            return false;
        }
        boolean valid = true;
        if(!root.isLeafNode()) {
            for(BPlusPointer bpp: root.getPointers()){
                if(!isValid(schema, getNode(schema, bpp.getPageIndex(), root.index), n)){
                    return false;
                }
            }
        }
        return true;
    }

    private void displayTree(TableSchema schema, BPlusNode<?> root, String prefix) {
        System.out.println(prefix + root);
        if(root.isLeafNode()) {
            return;
        }
        for (BPlusPointer bpp : root.getPointers()) {
            displayTree(schema, getNode(schema, bpp.getPageIndex(), root.index), prefix+" ");
        }
    }

    /**
     * Inserts a record at the end of the table, regardless of key ordering
     * @param schema The TableSchema of the table the record is being inserted into
     * @param record The record to insert
     */
    public void fastInsert(TableSchema schema, Record record) {
        // If table has no pages, make a new page and insert it into the buffer
        if (schema.rootIndex == -1) {
            Page firstPage = new Page(0, 0, schema);
            try {
                firstPage.save();
                buffer.insert(firstPage);
            } catch (IOException ioe) {
                System.err.println("Encountered exception while adding new page to table file: " + ioe.getMessage());
                return;
            }
        }
        // Get last page and insert record
        Page lastPage = getPage(schema, schema.pageCount() - 1);
        lastPage.records.add(record);
        schema.incrementRecordCount();

        // If the record is now oversize, remove and insert into a new page
        if (lastPage.pageDataSize() > catalog.pageSize()) {
            lastPage.records.removeLast();
            int pageIndex;
            try {
                pageIndex = addPage(schema.tableFile());
            } catch (IOException e) {
                System.err.println(e.getMessage());
                return;
            }
            // Create Page with new record and update catalog page mapping
            ArrayList<Record> recordList = new ArrayList<>();
            recordList.add(record);
            Page newPage = new Page(pageIndex, lastPage.index + 1, recordList, schema);
            schema.insertPage(newPage.pageNumber, newPage.index);
            // Insert the new page into the buffer
            try {
                buffer.insert(newPage);
                newPage.save();
            } catch (IOException ioe) {
                System.err.println("Failed to write split page to file. Error: " + ioe.getMessage());
            }
        }
    }

    /**
     * Creates a table with a given name in the catalog and creates a file for it. Primary key
     * requirements are not checked
     * @param tableName The name of the table
     * @param attributes The list of attributes in each record of the table
     * @throws IOException If an error is encountered when creating the table file
     */
    public TableSchema createTable(String tableName, ArrayList<Attribute> attributes) throws IOException {
        return catalog.createTableSchema(tableName, attributes);
    }

    /**
     * Removes a table from the database
     * @param tableName The name of the table to drop
     * @return 'true' if the table exists and was deleted
     */
    public boolean dropTable(String tableName) throws IOException {
        if (catalog.getTableSchema(tableName) == null) {
            System.err.println("Table '" + tableName + "' does not exist.");
        }
        // Clear buffer
        buffer.removeTable(tableName);
        File dataFile = new File(this.catalog.getFilePath().getParent() + File.separator + tableName + ".bin");
        try {
            if (!dataFile.delete()) { return false; }
        } catch (Exception e) {
            throw new IOException("Encountered an error while deleting table file:" + e.getMessage());
        }
        catalog.removeTableSchema(tableName);
        catalog.save();
        return true;
    }

    /**
     * Removes a page from a table
     * @param page The page object being removed
     */
    public void dropPage(Page page) {
        TableSchema schema = page.getTableSchema();
        // Remove page
        buffer.remove(page);
        schema.removePage(page.pageNumber);  // Decrements existing pages as well
        schema.decrementPageCount();
        // If page was root, get the new page 0 and set it as root
        if (page.pageNumber == 0) {
            schema.rootIndex = schema.getIndex(0);
        }
        buffer.refreshPageNumbers(schema); // Resync pageNumbers for buffered pages
    }

    /**
     * Replaces one table with another. The target table's data will be dropped, and the source
     * table will be renamed to the target's name.
     * @param targetSchema The TableSchema of the table that will be replaced by the source table
     * @param sourceSchema The TableSchema of the table whose data will be preserved
     */
    public void replaceTable(TableSchema targetSchema, TableSchema sourceSchema) {
        // Update the buffer, removing pages that belonged to the target and updating the schema for the source pages
        buffer.removeTable(targetSchema.name);
        // Update the schema in the catalog
        File oldSourceFile = sourceSchema.tableFile(); // This changes when you update the schema name
        catalog.removeTableSchema(sourceSchema.name);
        sourceSchema.name = targetSchema.name;
        catalog.setTableSchema(targetSchema.name, sourceSchema);
        // Verify both files exist before doing anything destructive
        File targetFile = targetSchema.tableFile();
        if (!targetFile.exists()) {
            System.err.println("Could not locate table file `" + targetFile.getAbsolutePath() + "`");
            return;
        }
        if (!oldSourceFile.exists()) {
            System.err.println("Could not locate table file `" + oldSourceFile.getAbsolutePath() + "`");
            return;
        }
        // Delete target file and rename source file
        if (!targetFile.delete()) {
            System.err.println("Failed to delete table file `" + targetFile.getAbsolutePath() + "`");
            return;
        }
        if (!oldSourceFile.renameTo(targetFile)) {
            System.err.println("Failed to rename table file `" + oldSourceFile.getAbsolutePath() +
                    "` to `" + targetFile.getAbsolutePath() + "'");
        }
    }

    /**
     * Prints the information of a table to the console
     * <ul>
     *     <li>Table name</li>
     *     <li>Table schema</li>
     *     <li># Pages</li>
     *     <li># Records</li>
     * </ul>
     * @param tableName The name of the table to print the details of
     */
    public void displayTable(String tableName){
        TableSchema schema = catalog.getTableSchema(tableName);
        if (schema == null) {
            System.err.println("Table not found: '" + tableName + "'");
            return;
        }
        System.out.println("Table name: " + tableName);
        System.out.println("Table schema: ");
        System.out.println(schema);
        System.out.println("Pages: " + schema.pageCount());
        System.out.println("Records: " + schema.recordCount());
    }

    /**
     * Displays the schema of the database to the console
     * <ul>
     *     <li>Database location</li>
     *     <li>Page size</li>
     *     <li>Buffer size</li>
     *     <li>Table schemas</li>
     * </ul>
     */
    public void displaySchema() {
        System.out.println("Database location: " + catalog.getFilePath().getAbsolutePath());
        System.out.println("Page size: " + catalog.pageSize());
        System.out.println("Buffer size: " + buffer.size());
        if (catalog.getTableNames().isEmpty()) {
            System.out.println("No Tables to Display");
        }
        else{
            System.out.println("\nTables:\n");
            for(String table : catalog.getTableNames()) {
                displayTable(table);
                System.out.println();
            }
        }
    }

    /**
     * Gets the TableSchema for the table with a given name
     * @param tableName The name of the table
     * @return The table's schema
     * @throws InvalidTableException if table does not exist in the catalog
     */
    public TableSchema getTableSchema(String tableName) throws InvalidTableException {
        return catalog.getTableSchema(tableName);
    }

    /**
     * Adds a single page to the given file. Returns the index of the page that would occupy the added space
     * @param file the file to add a page to
     * @return The index of the added page
     */
    private int addPage(File file) throws IOException {
        int newIndex = (int) file.length() / catalog.pageSize();  // Calculate index before expanding table
        try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
            out.seek(file.length());
            out.write(new byte[catalog.pageSize()]);
        } catch (FileNotFoundException fnf) {
            throw new IOException("Could not locate file `" + file.getAbsolutePath() + "`");
        }
        return newIndex;
    }

    /**
     * Unfreezes all pages in the buffer
     */
    public void unfreezeAllPages() {
        buffer.unfreezeAllPages();
    }

    /**
     * Writes the buffer and catalog to disk
     */
    public void save() {
        try {
            catalog.save();
        } catch (IOException e) {
            System.err.println("ERROR: Failed to save catalog to disk: " + e.getMessage());
        }
        try {
            buffer.save();
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    /**
     * Finds and deletes all tables in the database that start with a numeric character
     * This is mostly used for crash recovery, since all temp tables are supposed to be deleted after use.
     * In the event that the program closes/crashes before it can delete these, this will avoid the
     * program crashing upon the first attempt to create a temp table on next run
     * Query-based table files cannot have names starting with a number, so this will only delete temp tables.
     */
    public void wipeTempTables() throws IOException {
        File dbDirectory = catalog.getFilePath().getParentFile();
        File[] fileList = dbDirectory.listFiles();
        if (fileList == null) {
            return;  // This should never happen, but it makes the compiler happy
        }
        for (File file : fileList) {
            if(Character.isDigit(file.getName().charAt(0))){
                dropTable(file.getName().substring(0, file.getName().indexOf('.')));
            }
        }
    }

    /**
     * "Nukes" the database by deleting all files in the database directory
     */
    public void nuke() {
        System.err.println("\nNUKING DATABASE AT "+catalog.getFilePath().getParentFile().getAbsolutePath());
        quietNuke();
    }

    /**
     *  Nukes the database without the fanfare. Used to hide the double call added when Will made file reading commands a thing
     */
    public void quietNuke(){
        File dbDirectory = catalog.getFilePath().getParentFile();
        File[] fileList = dbDirectory.listFiles();
        if (fileList == null) {
            return;  // This should never happen, but it makes the compiler happy
        }
        for (File file : fileList) {
            if (!file.delete()) {
                System.err.println("Failed to delete file " + file.getAbsolutePath());
            }
        }
        catalog.getFilePath().getParentFile().delete();
    }

    /**
     * Toggles "NUKE MODE", which deletes all files in the DB directory upon exit
     */
    public void toggleNUKE_MODE() {
        NUKE_MODE = !NUKE_MODE;

        System.err.println("\nNUKE MODE "+((NUKE_MODE)? "enabled. Entire database will be deleted on program close.\n" : "disabled. Database will be saved as usual.\n"));
        try {
            TimeUnit.MILLISECONDS.sleep(25);    // allows stderr time to print
        } catch (InterruptedException e) {
            System.err.println(e + " : " + e.getMessage());
        }
    }

    /**
     * @return `true` if NUKE_MODE is enabled
     */
    public boolean inNUKE_MODE() {
        return NUKE_MODE;
    }

    public void test() {
        TableSchema fooSchema = catalog.getTableSchema("foo");
        ArrayList<BPlusPointer<Integer>> pointerList = new ArrayList<>();
        pointerList.add(new BPlusPointer<>(1, 0, 0));
        pointerList.add(new BPlusPointer<>(null, -1, -1));
        BPlusNode<Integer> root = new BPlusNode<>(fooSchema, 0, pointerList, -1);
        try {
            addPage(fooSchema.indexFile());
            System.out.println("Inserting root...");
            buffer.insert(root);
            System.out.println("Saving root...");
            buffer.save();
            System.out.println("Loading root...");
            BPlusNode<?> loadedRoot = buffer.getNode(fooSchema, 0, -1);
            System.out.println("root: " + root);
            System.out.println("loaded root: " + loadedRoot);
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
            for (StackTraceElement element : ioe.getStackTrace()) {
                System.err.println(element);
            }
        }
    }
}
