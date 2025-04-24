package components;

import bplus.BPlusNode;
import exceptions.CustomExceptions.*;
import tableData.Bufferable;
import tableData.Catalog;
import tableData.Page;
import tableData.TableSchema;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.ArrayList;

/**
 * Class representing the page buffer
 */
public class Buffer {

    int bufferSize;
    ArrayDeque<Bufferable> buffer;
    int pageSize;
    Catalog catalog;

    /**
     * Creates a new buffer
     * @param bufferSize The number of pages the buffer can store
     */
    public Buffer(Catalog catalog, int bufferSize, int pageSize) {
        this.bufferSize = bufferSize;
        buffer = new ArrayDeque<>(bufferSize);
        this.pageSize = pageSize;
        this.catalog = catalog;
    }

    /**
     * The number of pages that the buffer can hold simultaneously
     * @return The buffer size
     */
    public int size() {
        return bufferSize;
    }

    /**
     * Inserts an item into the buffer, popping another element if the buffer is full
     * @param page The element to insert
     * @throws IOException if the popped element could not be written back to disk
     */
    public void insert(Bufferable page) throws IOException {
        // See if we need to make room in the buffer
        if (buffer.size() >= bufferSize) {
            ArrayList<Bufferable> pageStack = new ArrayList<>();
            Bufferable old = buffer.removeLast();
            // Loop until you either find an unfrozen page or empty the buffer
            while (!buffer.isEmpty()) {
                if (old.isFrozen()) {
                    pageStack.add(old);
                } else {
                    break;
                }
                old = buffer.removeLast();
            }
            if (old.isFrozen()) {
                // If every page in the buffer is frozen, there's nothing you can do
                throw new PageFreezeException("Attempted to insert a page into a buffer, but all pages were frozen");
            }
            old.save();
            // Push the stack back into the buffer in order
            while (!pageStack.isEmpty()) {
                buffer.addLast(pageStack.removeLast());
            }
        }
        buffer.push(page);
    }

    /**
     * Removes an element from the buffer
     * @param page The element to remove
     */
    public void remove(Bufferable page) {
        buffer.remove(page);
    }

    // ====================================================================================
    //region Page =========================================================================
    // ====================================================================================

    /**
     * Returns a given page number from a table. It will be retrieved from the buffer,
     * unless the page isn't present, in which case it will be fetched from storage
     * @param schema The schema of the table to fetch the page from
     * @param pageIndex The index of the page being fetched
     * @return The requested Page. `null` if pageIndex does not correspond to a real page
     */
    public Page getPage(TableSchema schema, int pageIndex) {
        if (pageIndex == -1 || schema.getPageNumber(pageIndex) == -1) {
            return null;
        }
        // Search the buffer for the page and return it
        for (Bufferable page : buffer) {
            // We're only looking for pages
            if (page.getClass() != Page.class) {
                continue;
            }
            if (page.matchesSchema(schema)) {
                if (page.index == pageIndex) {
                    // If page was found, move it to the back of the queue and return it
                    buffer.remove(page);
                    buffer.push(page);
                    return (Page)page;
                }
            }
        }
        // Page wasn't in the buffer, so load it in.
        return loadPage(schema, pageIndex);


//        // First, if requested page was page 0, just load it from rootIndex and return
//        if (pageNum == 0) {
//            return loadPage(schema, schema.rootIndex, 0);
//        }
//        // Otherwise, find page offset by hunting from the closest page.
//        // If no page from this table was in the buffer or the beginning is closer, start from the beginning
//        if (currClosest == null || Math.abs(currClosest.index - pageNum) > pageNum) {
//            currClosest = loadPage(schema, schema.rootIndex, 0);
//            if (currClosest == null) {
//                // If the first page cannot be loaded, table must have zero pages
//                return null;
//            }
//        }
//        HashSet<Integer> visitedIndices = new HashSet<>();
//        visitedIndices.add(currClosest.pageIndex);
//        int currPageNumber = currClosest.index;
//        int nextIndex;
//        // Hop from page to page until you get to the target
//        while (true) {
//            // Update nextIndex and currPageNumber depending on the direction
//            if (currPageNumber < pageNum) {
//                nextIndex = currClosest.nextPage;
//                currPageNumber += 1;
//            } else {
//                nextIndex = currClosest.prevPage;
//                currPageNumber -= 1;
//            }
//            // Error checking
//            if (visitedIndices.contains(nextIndex)) {
//                System.err.println("Found reference loop with page indices " + visitedIndices +
//                        " in table `" + schema.name + "`, ending at " + nextIndex);
//            }
//            visitedIndices.add(nextIndex);
//            // Load next page, unless you've run out of pages to loop through
//            try {
//                currClosest = loadPage(schema, nextIndex, currPageNumber);
//            } catch (IndexOutOfBoundsException ioob) {
//                return null;
//            }
//            if (currClosest == null) {
//                return null;
//            } else if (currPageNumber == pageNum) {
//                return currClosest;
//            }
//        }
    }

    /**
     * Loads a specific page into the buffer, freeing up an existing page if space is needed.
     * NOTE: This function <b>does not</b> check if the page already exists in the buffer.
     * @param schema The TableSchema of the table the Page belongs to
     * @param pageIndex The index of the page within the table's file, irrespective of the page's
     *                  actual number (i.e. pageIndex * pageSize = the byte offset of the desired page)
     * @return A reference to the Page that was inserted
     * @throws IndexOutOfBoundsException if pageIndex is outside the bounds of the table file
     */
    public Page loadPage(TableSchema schema, int pageIndex) throws IndexOutOfBoundsException {
        byte[] pageData = new byte[pageSize];
        File tableFile = schema.tableFile();
        if (!tableFile.exists()) {
            System.err.println("Could not find table file.");
            return null;
        }
//        // Make sure the file contains a page with that index
//        if ((pageIndex + 1) > tableFile.length() / pageSize) {  // This breaks if page size is less than the table offset
//            return null;
//        } else if (pageIndex < 0) {
//            throw new IndexOutOfBoundsException("Invalid page index `" + pageIndex + "`");
//        }

        // Read in the data
        try (RandomAccessFile raf = new RandomAccessFile(tableFile, "r")) {
            long offset = Integer.BYTES + ((long) pageIndex * pageSize);  // Page count + pageIndex offset
            raf.seek(offset);
            if (raf.read(pageData) != pageSize) {
                System.err.println("WARNING: Read fewer bytes than expected while loading page from `" +
                        tableFile.getAbsolutePath() + "`");
            }
        } catch (IOException ioe) {
            System.err.println("Encountered problem while attempting to read table file: " + ioe.getMessage());
            return null;
        }
        // Parse the page data and return it
        try {
            int pageNum = schema.getPageNumber(pageIndex);
            Page newPage = new Page(pageIndex, pageNum, pageData, schema);
            insert(newPage);
            return newPage;
        } catch (IOException ioe) {
            System.err.println("Failed to parse page at index " + pageIndex +
                    " for table `" + schema.name + "` with error: " + ioe);
            return null;
        }
    }

    /**
     * Verifies that all pages in the buffer have the appropriate page number for their index.
     * Used after inserting/removing a page from a table, which shifts page numbers
     * @param schema The TableSchema of the table the pages being updated belong to
     */
    public void refreshPageNumbers(TableSchema schema) {
        for (int i = 0; i < buffer.size(); i++) {
            Bufferable bPage = buffer.pop(); // Pop next page
            if (bPage.getClass() != Page.class) {
                buffer.addLast(bPage);
                continue;
            }
            Page page = (Page) bPage;
            if (page.matchesSchema(schema)) {
                page.pageNumber = schema.getPageNumber(page.index);
            }
            buffer.addLast(page);
        }
    }

    //endregion

    // ====================================================================================
    //region BPlusTree ====================================================================
    // ====================================================================================

    /**
     * Returns a BPlus Tree Node with a given ID from a specified table. It will be retrieved
     * from the buffer unless the node isn't present, in which case it will be fetched from storage
     * @param schema The schema of the table to fetch the node from
     * @param nodeIndex The index of the page to fetch
     * @param parent The parent of this node. `null` for root nodes
     * @return The requested BPlusNode
     * @throws IndexOutOfBoundsException if nodeIndex is outside the bounds of the B+ Tree file
     */
    public BPlusNode<?> getNode(TableSchema schema, int nodeIndex, BPlusNode<?> parent) throws IndexOutOfBoundsException {
        // Search the buffer for the page and return it
        for (Bufferable node : buffer) {
            // We're only looking for pages
            if (node.getClass() == Page.class) {
                continue;
            }
            if (node.match(schema, nodeIndex)) {
                if (node.index == nodeIndex) {
                    // If page was found, move it to the back of the queue and return it
                    buffer.remove(node);
                    buffer.push(node);
                    return (BPlusNode<?>) node;
                }
            }
        }
        // Page wasn't in the buffer, so load it in.
        return loadNode(schema, nodeIndex, parent);
    }

    /**
     * Loads a specific page into the buffer, freeing up an existing page if space is needed.
     * NOTE: This function <b>does not</b> check if the page already exists in the buffer.
     * @param schema The TableSchema of the table the Page belongs to
     * @param nodeIndex The index of the node within the tree's file (i.e. nodeIndex * pageSize =
     *                  the byte offset of the desired node)
     * @param parent The parent of the node being loaded; `null` if node is root
     * @return A reference to the Node that was inserted
     * @throws IndexOutOfBoundsException if pageIndex exceeds the size of the table file
     */
    public BPlusNode<?> loadNode(TableSchema schema, int nodeIndex, BPlusNode<?> parent) throws IndexOutOfBoundsException {
        byte[] nodeData = new byte[pageSize];
        File indexFile = schema.indexFile();
        if (!indexFile.exists()) {
            System.err.println("Could not find table file.");
            return null;
        }
        // Make sure index is within the bounds of the file
        if ((nodeIndex + 1) > indexFile.length() / pageSize) {  // This breaks if pageSize is less than the table offset
            return null;
        } else if (nodeIndex < 0) {
            throw new IndexOutOfBoundsException("Invalid node index `" + nodeIndex + "`");
        }
        // Read in the data
        try (RandomAccessFile raf = new RandomAccessFile(indexFile, "r")) {
            long offset = Integer.BYTES + ((long) nodeIndex * pageSize);  // Page count + nodeIndex offset
            raf.seek(offset);
            if (raf.read(nodeData) != pageSize) {
                System.err.println("WARNING: Read fewer bytes than expected while loading node from `" +
                        indexFile.getAbsolutePath() + "`");
            }
        } catch (IOException ioe) {
            System.err.println("Encountered problem while attempting to read index file: " + ioe.getMessage());
            return null;
        }
        // Parse the node data and return it
        try {
            BPlusNode<?> newNode = BPlusNode.parse(schema, nodeIndex, nodeData, parent);
            insert(newNode);
            return newNode;
        } catch (IOException ioe) {
            System.err.println("Failed to parse node " + nodeIndex + " for table `" +
                    schema.name + "` with error: " + ioe);
            return null;
        }
    }

    //endregion

    /**
     * Removes all Pages belonging to a given table from the buffer without saving them to disk
     * @param tableName The name of the table whose Pages are being dropped from the filter
     */
    public void removeTable(String tableName) {
        ArrayDeque<Bufferable> newBuffer = new ArrayDeque<>(buffer.size());
        while (!buffer.isEmpty()) {
            Bufferable currPage = buffer.removeLast();
            if (currPage.getTableName().equals(tableName)) {
                continue;  // Pages from the target are not carried over
            }
            // Push the page onto the other end of the queue
            newBuffer.push(currPage);
        }
        buffer = newBuffer;
    }

    /**
     * Unfreezes all pages in the buffer
     */
    public void unfreezeAllPages() {
        for (Bufferable page : buffer) {
            while (page.isFrozen()) {
                page.unfreeze();
            }
        }
    }

    /**
     * Writes the contents of this buffer out to disk. Calling this method empties the buffer
     * @throws IOException if a problem occurred while writing to disk
     */
    public void save() throws IOException {
        while (!buffer.isEmpty()) {
            buffer.pop().save();
        }
    }
}
