package components;

import tableData.Page;
import tableData.TableSchema;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.HashSet;

/**
 * Class representing the page buffer
 */
public class Buffer {

    int bufferSize;
    ArrayDeque<Page> buffer;
    int pageSize;

    /**
     * Creates a new buffer
     * @param bufferSize The number of pages the buffer can store
     */
    public Buffer(int bufferSize, int pageSize) {
        this.bufferSize = bufferSize;
        buffer = new ArrayDeque<>(bufferSize);
        this.pageSize = pageSize;
    }

    /**
     * Returns the number of pages currently stored in the buffer
     * @return The number of pages in the buffer
     */
    public int count() {
        return buffer.size();
    }

    /**
     * The number of pages that the buffer can hold simultaneously
     * @return The buffer size
     */
    public int size() {
        return bufferSize;
    }

    /**
     * Returns a page with a given ID from a specified table. It will be retrieved from the buffer,
     * unless the page isn't present, in which case it will be fetched from storage
     * @param schema The schema of the table to fetch the page from
     * @param pageNum The number of the page to fetch
     * @return The requested Page. `null` if page number exceeds the number of pages in the table
     */
    public Page getPage(TableSchema schema, int pageNum) {
        // If table has no pages, return null
        if (schema.rootIndex == -1) {
            return null;
        }
        // Search the buffer for the page and return it
        Page currClosest = null;
        for (Page page : buffer) {
            if (page.getTableName().equals(schema.name)) {
                if (page.pageNumber == pageNum) {
                    // If page was found, move it to the back of the queue and return it
                    buffer.remove(page);
                    buffer.push(page);
                    return page;
                }
                // Check if this ID was at least closer than the previous target
                if (currClosest == null ||
                        Math.abs(page.pageNumber - pageNum) < Math.abs(currClosest.pageNumber - pageNum)) {
                    currClosest = page;
                }
            }
        }
        // Page wasn't in the buffer, so load it in.
        // First, if requested page was page 0, just load it from rootIndex return
        if (pageNum == 0) {
            return loadPage(schema, schema.rootIndex, 0);
        }
        // Otherwise, find page offset by hunting from the closest page.
        // If no page from this table was in the buffer, start from the beginning
        if (currClosest == null) {
            currClosest = loadPage(schema, schema.rootIndex, 0);
            if (currClosest == null) {
                // If the first page cannot be loaded, table must have zero pages
                return null;
            }
        }
        HashSet<Integer> visitedIndices = new HashSet<>();
        visitedIndices.add(currClosest.pageIndex);
        int currPageNumber = currClosest.pageNumber;
        int nextIndex;
        // Hop from page to page until you get to the target
        while (true) {
            // Update nextIndex and currPageNumber depending on the direction
            if (currPageNumber < pageNum) {
                nextIndex = currClosest.nextPage;
                currPageNumber += 1;
            } else {
                nextIndex = currClosest.prevPage;
                currPageNumber -= 1;
            }
            // Error checking
            if (visitedIndices.contains(nextIndex)) {
                System.err.println("Found reference loop with page indices " + visitedIndices +
                        " in table `" + schema.name + "`, ending at " + nextIndex);
            }
            visitedIndices.add(nextIndex);
            // Load next page, unless you've run out of pages to loop through
            try {
                currClosest = loadPage(schema, nextIndex, currPageNumber);
            } catch (IndexOutOfBoundsException ioob) {
                return null;
            }
            if (currClosest == null) {
                return null;
            } else if (currPageNumber == pageNum) {
                return currClosest;
            }
        }
    }

    /**
     * Loads a specific page into the buffer, freeing up an existing page if space is needed.
     * NOTE: This function <b>does not</b> check if the page already exists in the buffer.
     * @param schema The TableSchema of the table the Page belongs to
     * @param pageIndex The index of the page within the table's file, irrespective of the page's
     *                  actual number (i.e. pageIndex * pageSize = the byte offset of the desired page)
     * @param pageNum The page number that should be assigned to this page. If there are gaps in
     *                a table file or the pages are out of order, this number will be different from
     *                pageIndex.
     * @return A reference to the Page that was inserted. If pageIndex exceeds the size of the
     * table file, returns `null`
     * @throws IndexOutOfBoundsException if pageIndex exceeds the size of the table file
     */
    public Page loadPage(TableSchema schema, int pageIndex, int pageNum) throws IndexOutOfBoundsException {
        byte[] pageData = new byte[pageSize];
        File tableFile = schema.tableFile();
        if (!tableFile.exists()) {
            System.err.println("Could not find table file.");
            return null;
        }
        // Make sure the file contains a page with that index
        if ((pageIndex + 1) > tableFile.length() / pageSize) {  // This breaks if page size is less than the table offset
            return null;
        } else if (pageIndex < 0) {
            throw new IndexOutOfBoundsException("Invalid page index `" + pageIndex + "`");
        }
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
            Page newPage = new Page(pageIndex, pageNum, pageData, schema);
            insertPage(newPage);
            return newPage;
        } catch (IOException ioe) {
            System.err.println("Failed to parse page " + pageNum + " at index " + pageIndex +
                    " for table `" + schema.name + "` with error: " + ioe);
            return null;
        }
    }

    /**
     * Inserts a Page into the buffer, popping another Page if the buffer is full
     * @param page The page to insert
     * @throws IOException if the popped page could not be written back to disk
     */
    public void insertPage(Page page) throws IOException {
        Page old = null;
        // See if we need to make room in the buffer
        if (buffer.size() >= bufferSize) {
            old = buffer.removeLast();
            savePage(old);
        }
        buffer.push(page);
    }

    /**
     * Removes all Pages belonging to a given table from the buffer without saving them to disk
     * @param tableName The name of the table whose Pages are being dropped from the filter
     */
    public void removeTable(String tableName) {
        ArrayDeque<Page> newBuffer = new ArrayDeque<>(buffer.size());
        while (!buffer.isEmpty()) {
            Page currPage = buffer.removeLast();
            if (currPage.getTableName().equals(tableName)) {
                continue;  // Pages from the target are dropped
            }
            // Push the page onto the other end of the queue
            newBuffer.push(currPage);
        }
        buffer = newBuffer;
    }

    /**
     * Replaces the TableSchema for all Pages belonging to a specific table
     * @param tableName The name of table the pages being updated belong to
     * @param newSchema The new TableSchema for those pages
     */
    public void updateSchema(String tableName, TableSchema newSchema) {
        for (Page page : buffer) {
            if (page.getTableSchema().name.equals(tableName)) {
                page.updateSchema(newSchema);
            }
        }
    }

    /**
     * Increments the page numbers of all pages above a certain number
     * @param tableName The name of the table the pages being updated belong to
     * @param above The threshold (inclusive) above which to increment the page numbers
     */
    public void incrementPageNumbers(String tableName, int above) {
        for (Page page : buffer) {
            if (page.getTableName().equals(tableName) && page.pageNumber >= above) {
                page.pageNumber += 1;
            }
        }
    }

    /**
     * Saves a Page to its corresponding table file
     * @param page The Page to write
     * @throws IOException if there is an error writing the Page to file
     */
    public void savePage(Page page) throws IOException {
        // Read in the data
        File tableFile = page.getTableSchema().tableFile();
        if (!tableFile.exists()) {
            throw new IOException("Could not find table file `" + tableFile.getAbsolutePath() + "`");
        }
        try (RandomAccessFile raf = new RandomAccessFile(tableFile, "rw")) {
            long offset = Integer.BYTES + ((long) page.pageIndex * pageSize);  // Page count + pageIndex offset
            raf.seek(offset);
            byte[] pageData = page.encodePage();
            if (pageData.length > pageSize) {
                System.err.println("Page data array exceeded pageSize while saving");
            }
            raf.write(pageData);
        } catch (IOException ioe) {
            throw new IOException("Encountered problem while attempting to write to table file: " + ioe.getMessage());
        }
    }

    /**
     * Writes the contents of this buffer out to disk. Calling this method empties the buffer
     * @throws IOException if a problem occurred while writing to disk
     */
    public void save() throws IOException {
        while (!buffer.isEmpty()) {
            savePage(buffer.pop());
        }
    }
}
