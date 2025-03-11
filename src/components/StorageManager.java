package components;

import tableData.*;
import tableData.Record;
import java.io.*;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.HashSet;

/**
 * StorageManager.java
 * Manages fetching and saving pages to file
 */
public class StorageManager {

    int bufferSize;
    ArrayDeque<Page> buffer;
    Catalog catalog;

    int pageSize;

    /**
     * Creates a StorageManager object
     * @param databaseDir A File object pointing to the directory where the database files are stored
     * @param pageSize The page size used. If a catalog already exists, the page size of that catalog
     *                 will be used instead
     * @param bufferSize The size of the page buffer
     * @throws IOException If there are problems accessing or modifying the catalog and table files
     */
    public StorageManager(File databaseDir, int pageSize, int bufferSize) throws IOException {
        this.bufferSize = bufferSize;
        buffer = new ArrayDeque<>(bufferSize);
        File catalogFile = new File(databaseDir, "catalog.bin");
        catalog = new Catalog(catalogFile, pageSize);
        this.pageSize = pageSize;
    }

    /**
     * Returns a page with a given ID from a specified table. It will be retrieved from the buffer,
     * unless the page isn't present, in which case it will be fetched from storage
     * @param tableName The name of the table to fetch the page from
     * @param pageNum The number of the page to fetch
     * @return The requested Page. `null` if page number exceeds the number of pages in the table
     */
    public Page getPage(String tableName, int pageNum) {
        // Search the buffer for the page and return it
        Page currClosest = null;
        for (Page page : buffer) {
            if (page.getTableName().equals(tableName)) {
                if (page.pageNumber == pageNum) {
                    // Move page to the back of the queue and return it
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
            return loadPage(tableName, getTableSchema(tableName).rootIndex, 0);
        }
        // Otherwise, find page offset by hunting from the closest page.
        // If no page from this table was in the buffer, start from the beginning
        if (currClosest == null) {
            currClosest = loadPage(tableName, getTableSchema(tableName).rootIndex, 0);
            if (currClosest == null) {
                // If the first page cannot be loaded, table must have zero pages
                return null;
            }
        }
        HashSet<Integer> visitedIndices = new HashSet<>();
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
            if (visitedIndices.contains(currClosest.pageNumber)) {
                System.err.println("Found reference loop with pages indices " + visitedIndices +
                        " in table `" + tableName + "`");
            }
            visitedIndices.add(nextIndex);
            // Load next page, unless you've run out of pages to loop through
            try {
                currClosest = loadPage(tableName, nextIndex, currPageNumber);
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
     * @param tableName The ID of the table the Page belongs to
     * @param pageIndex The index of the page within the table's file, irrespective of the page's
     *                  actual number (i.e. pageIndex * pageSize = the byte offset of the desired page)
     * @param pageNum The page number that should be assigned to this page. If there are gaps in
     *                a table file or the pages are out of order, this number will be different from
     *                pageIndex.
     * @return A reference to the Page that was inserted. If pageIndex exceeds the size of the
     * table file, returns `null`
     * @throws IndexOutOfBoundsException if pageIndex exceeds the size of the table file
     */
    private Page loadPage(String tableName, int pageIndex, int pageNum) throws IndexOutOfBoundsException {
        byte[] pageData = new byte[pageSize];
        File tableFile = catalog.getTableFile(tableName);
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
            Page newPage = new Page(pageIndex, pageNum, pageData, catalog.getTableSchema(tableName));
            insertPage(newPage);
            return newPage;
        } catch (IOException ioe) {
            System.err.println("Failed to parse page " + pageNum + " at index " + pageIndex +
                    " for table `" + tableName + "` with error: " + ioe);
            return null;
        }
    }

    /**
     * Inserts a Page into the buffer, popping another Page if the buffer is full and saving it to disk
     * @param page The page to insert
     * @return `true` if a page was dropped from the buffer to make room
     */
    private boolean insertPage(Page page) {
        Page old = null;
        // See if we need to make room in the buffer
        if (buffer.size() >= bufferSize) {
            old = buffer.removeLast();
            savePage(old);
        }
        buffer.push(page);
        return old != null;
    }

    /**
     * Gets record from a particular table by primary key
     * @param tableName name of table
     * @param key string of key to search
     * @return record with matching key (or null if no matches)
     */
    public Record getByPrimaryKey(String tableName, String key)  {
        TableSchema tschema = catalog.getTableSchema(tableName);
        //finding the attribute index with the primary key
        int primIndex = tschema.getAttributeIndex(tschema.primaryKey);
        //looping through pages and records to find The One
        Page currPage;
        int pageNum = 0;
        while (true) {
            currPage = getPage(tableName, pageNum);
            pageNum += 1;
            if (currPage == null) {  // Reached end of table without finding the list
                return null;
            }
            for (Record r : currPage.getRecords()) {
                if (r.get(primIndex).equals(key)) {
                    return r;
                }
            }
        }
    }

    /**
     * Inserts a record into a given table
     * @param tableName The name of the table to insert the records into
     * @param record The record to insert
     */
    public void insertRecord(String tableName, Record record) {
        // Get the pages for that table
        TableSchema tschema = catalog.getTableSchema(tableName);
        if (tschema == null) {
            throw new IllegalArgumentException("Table '" + tableName + "' does not exist.");
        }
        // If table has no pages, make a new page and insert it into the buffer
        if (tschema.rootIndex == -1) {
            Page firstPage = new Page(0, 0, tschema, pageSize);
            savePage(firstPage);
            insertPage(firstPage);
        }
        // Iterate over pages until you find the one where the record goes
        int currIndex = 0;
        Page currPage = getPage(tableName, 0);
        int recordIndex = 0;
        // Loop over all records and all pages until you find the insertion point
        pageLoop:
        while (true) {
            // Iterate over the records until you find the one that comes after the new record in order
            for (int i = 0; i < currPage.recordCount(); i++) {
                Record pageRecord = currPage.records.get(i);
                recordIndex = i;
                // If new record goes before pageRecord or you hit the end of the table,
                // insert it into the page at that index
                if (pageRecord.compareTo(record, tschema) >= 0) {
                    break pageLoop;
                }
            }
            // If there is no page after this one, break and insert into the last page
            if (currPage.nextPage == -1) {
                recordIndex = currPage.recordCount();
                break;
            }
            currIndex += 1;
            currPage = getPage(tableName, currIndex);
        }
        // At this point, currPage is the page to insert into and recordIndex is the index to insert into
        currPage.records.add(recordIndex, record);
        // If the record is now oversize, split
        if (currPage.pageDataSize() > pageSize) {
            int pageIndex;
            try {
                pageIndex = expandTable(tableName);
            } catch (IOException e) {
                // If there was a failure, undo the record insert and abort
                System.err.println(e.getMessage());
                currPage.records.remove(recordIndex);
                return;
            }
            Page child = currPage.split(pageIndex);
            if (child.nextPage != -1) {
                // Update the prevPage pointer for the page after this one, if one exists
                Page afterChild = getPage(tableName, currPage.pageNumber + 1);
                afterChild.prevPage = pageIndex;
                // Increment the page number for every page that follows child
                for (Page page : buffer) {
                    if (page.getTableName().equals(tableName) && page.pageNumber >= currPage.pageNumber) {
                        page.pageNumber += 1;
                    }
                }
            }
            // Insert the new page into the buffer
            insertPage(child);
            savePage(child);
        }
    }

    public boolean deleteByPrimaryKey(String tableName, String key){
        TableSchema tschema = catalog.getTableSchema(tableName);
        //finding the attribute index with the primary key
        int primIndex = tschema.getAttributeIndex(tschema.primaryKey);
        //looping through pages and records to find The One
        Page currPage;
        int pageNum = 0;
        while (true) {
            currPage = getPage(tableName, pageNum);
            pageNum += 1;
            if (currPage == null) {  // Reached end of table without finding the record
                return false;
            }
            // Loop over the page, removing the record if you find it
            for (int i = 0; i < currPage.recordCount(); i++) {
                Record r = currPage.records.get(i);
                if (r.get(primIndex).equals(key)) {
                    currPage.records.remove(i);
                    return true;
                }
            }
        }
    }

    public boolean updateByPrimaryKey(String tableName, String key, Record record){
        TableSchema tschema = catalog.getTableSchema(tableName);
        //finding the attribute index with the primary key
        int primIndex = tschema.getAttributeIndex(tschema.primaryKey);
        //looping through pages and records to find The One
        Page currPage;
        int pageNum = 0;
        while (true) {
            currPage = getPage(tableName, pageNum);
            pageNum += 1;
            if (currPage == null) {  // Reached end of table without finding the record
                return false;
            }
            // Loop over the page, removing the record if you find it
            for (int i = 0; i < currPage.recordCount(); i++) {
                Record r = currPage.records.get(i);
                if (r.get(primIndex).equals(key)) {
                    currPage.records.set(i, record);
                    return true;
                }
            }
        }
    }

    /**
     * Creates a table with a given name in the catalog and creates a file for it
     * @param tableName The name of the table
     * @param attributes The list of attributes in each record of the table
     * @throws IOException If an error is encountered when creating the table file
     */
    public void createTable(String tableName, ArrayList<Attribute> attributes) throws IOException {
        if (catalog.addTableSchema(new TableSchema(tableName, -1, attributes))) {
            File tableFile = new File(catalog.getFilePath().getParent() + File.separator + tableName + ".bin");
            if (!tableFile.createNewFile()) {
                throw new RuntimeException("File already exists for table '" + tableName + "' at '" + tableFile.getAbsolutePath() + "'");
            }
            try (FileOutputStream fs = new FileOutputStream(tableFile)) {
                try (DataOutputStream out = new DataOutputStream(fs)) {
                    out.writeInt(0); // Initial page count is zero
                    catalog.save();
                } catch (Exception e) {
                    throw new IOException("Encountered an error while creating table file:" + e.getMessage());
                }
            } catch (Exception e) {
                throw new IOException("Encountered an error while creating table file:" + e.getMessage());
            }
        }
        else {
            System.err.println("Table " + tableName + " already exists.");
        }
    }

    /**
     * Removes a table from the database
     * @param tableName The name of the table to drop
     * @return 'true' if the table exists and was deleted
     */
    public boolean deleteTable(String tableName) throws IOException {
        if (catalog.getTableSchema(tableName) == null) {
            System.err.println("Table '" + tableName + "' does not exist.");
        }
        File dataFile = new File(this.catalog.getFilePath().getParent() + File.separator + tableName + ".bin");
        try {
            if (!dataFile.delete()) { return false; }
        } catch (Exception e) {
            throw new IOException("Encountered an error while deleting table file:" + e.getMessage());
        }
        catalog.removeTableSchema(tableName);
        //TODO: Remove pages from buffer for dropped table
        catalog.save();
        return true;
    }

    /**
     * Adds a new attribute to every record in a table. An attribute cannot be added if:
     * <ul>
     *     <li>New attribute is a primary key</li>
     *     <li>An attribute with that name already exists in the table</li>
     *     <li>The attribute is 'notNull' but has a null default value</li>
     * </ul>
     * @param tableName The name of the table to modify
     * @param newAttribute The attribute to add
     * @return 'true' if the attribute was added to the table; 'false' if there was an error
     */
    public boolean addAttribute(String tableName, Attribute newAttribute) {
//        if (newAttribute.primaryKey) {
//            System.err.println("Cannot add primary key to an existing table.");
//            return false;
//        }
//        if (newAttribute.notNull && newAttribute.defaultValue == null) {
//            System.err.println("New attribute is 'notnull', but 'null' is the default value.");
//            return false;
//        }
//        ArrayList<Page> pageList = getPageList(tableName);
//        if (!buffer.containsKey(tableName)) {
//            System.err.println("Table " + tableName + " does not exist");
//            return false;
//        }
//        TableSchema schema = catalog.getTableSchema(tableName);
//        for (Attribute a : schema.attributes) {
//            if (a.name.equals(newAttribute.name)) {
//                System.err.println("Table " + tableName + " already contains attribute '" + newAttribute.name + "'.");
//                return false;
//            }
//        }
//        TableSchema newSchema = schema.duplicate();
//        newSchema.attributes.add(newAttribute);
//        for (Page p : pageList) {
//            p.updateSchema(newSchema);
//        }
//        // schema.attributes.add(newAttribute);
//        catalog.setTableSchema(tableName, newSchema);
//        System.out.println("Added attribute '" + newAttribute.name + "' to table '" + tableName + "'");
//        return true;
        //TODO: Replace implementation with temp table approach
        return false;
    }

    /**
     * Removes an attribute from a given table, so long as the attribute is not the primary key
     * @param tableName The name of the table to remove the attribute from
     * @param attributeName The name of the attribute to remove. Should be
     * @return 'true' if the attribute was successfully dropped; 'false' if there was an error
     */
    public boolean dropAttribute(String tableName, String attributeName) {
//        ArrayList<Page> pageList = getPageList(tableName);
//        if (!buffer.containsKey(tableName)) {
//            System.err.println("Table " + tableName + " does not exist");
//            return false;
//        }
//        TableSchema schema = catalog.getTableSchema(tableName);
//        ArrayList<Attribute> attributes = schema.attributes;
//        int attrIndex = schema.getAttributeIndex(attributeName);
//        if (attrIndex == -1) {
//            System.err.println("Cannot drop attribute '" + attributeName + "': no such attribute.");
//            return false;
//        }
//        if (attributes.get(attrIndex).primaryKey) {
//            System.err.println("Cannot drop attribute '" + attributeName + "': key is primary key.");
//            return false;
//        }
//        schema.attributes.remove(attrIndex);
//        for (Page p : pageList) {
//            p.updateSchema(schema);
//        }
//        catalog.setTableSchema(tableName, schema);
//        return true;
        //TODO: Replace implementation with temp table approach
        return false;
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
        int totalPages = 0;
        int totalRecords = 0;
        if (schema.rootIndex != -1) {
            Page currPage = getPage(tableName, 0);
            while (currPage != null) {
                totalRecords += currPage.recordCount();
                totalPages += 1;
                currPage = getPage(tableName, totalPages);
            }
        }
        System.out.println("Pages: " + totalPages);
        System.out.println("Records: " + totalRecords);
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
        System.out.println("Page size: " + pageSize);
        System.out.println("Buffer size: " + bufferSize);
        if (catalog.getTableNames().isEmpty()) {
            System.out.println("No Tables to Display");
        }
        else{
            System.out.println("\nTables:\n");
            for(String table : catalog.getTableNames()) {
                displayTable(table);
            }
        }
    }

    /**
     * Gets the TableSchema for the table with a given name
     * @param tableName The name of the table
     * @return The table's schema
     */
    public TableSchema getTableSchema(String tableName){
        return catalog.getTableSchema(tableName);
    }

    /**
     * Increases the size of a table file by a single page. Returns the index of the page
     * that would occupy the added space
     * @param tableName The name of the table to expand
     * @return The index of the added page
     */
    private int expandTable(String tableName) throws IOException {
        // Being a private function, you can assume this call does not return null
        File tableFile = catalog.getTableFile(tableName);
        int newIndex = (int) tableFile.length() / pageSize;  // Calculate index before expanding table
        try (RandomAccessFile out = new RandomAccessFile(tableFile, "rw")) {
            out.seek(tableFile.length());
            out.write(new byte[pageSize]);
        } catch (FileNotFoundException fnf) {
            throw new IOException("Could not locate table file for table `" + tableFile.getAbsolutePath() + "`");
        }
        return newIndex;
    }

    /**
     * Writes the buffer and catalog to disk
     * @return 'true' if this operation succeeded; 'false' if there was an error
     */
    public boolean save() {
        try {
            catalog.save();
        } catch (IOException e) {
            System.err.println("ERROR: Failed to save catalog to disk: " + e.getMessage());
        }
        while (!buffer.isEmpty()) {
            // savePage() returns `false` on a failure
            if (!savePage(buffer.pop())) {
                // Abort and propagate that error
                return false;
            }
        }
        return true;
    }

    /**
     * Saves a page to its corresponding table file
     * @param page The page to write
     * @return `false` if the table failed to be written to file
     */
    private boolean savePage(Page page) {
        // Read in the data
        File tableFile = catalog.getTableFile(page.getTableName());
        if (!tableFile.exists()) {
            System.err.println("Could not find table file.");
            return false;
        }
        try (RandomAccessFile raf = new RandomAccessFile(tableFile, "rw")) {
            long offset = Integer.BYTES + ((long) page.pageIndex * pageSize);  // Page count + pageIndex offset
            raf.seek(offset);
            raf.write(page.encodePage());
        } catch (IOException ioe) {
            System.err.println("Encountered problem while attempting to write to table file: " + ioe.getMessage());
            return false;
        }
        return true;
    }

    public void test(ArrayList<String> args) {
        String cmd = args.get(1);
        String val = args.get(2);
        int num;
        Page page;
        switch (cmd) {
            case "save":
                num = Integer.parseInt(val);
                page = getPage("foo", num);
                savePage(page);
                break;
            case "load":
                num = Integer.parseInt(val);
                loadPage("foo", num, num);
                break;
            case "print":
                num = Integer.parseInt(val);
                page = getPage("foo", num);
                System.out.println(page);
                break;
            case "flush":
                save();
                buffer.clear();
                break;
        }
    }
}
