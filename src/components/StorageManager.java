package components;

import tableData.*;
import tableData.Record;
import java.io.*;
import java.util.ArrayList;
import java.util.ArrayDeque;

/**
 * StorageManager.java
 * Manages fetching and saving pages to file
 */
public class StorageManager {

    private Buffer buffer;
    Catalog catalog;
    int nextTempID;

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
        buffer = new Buffer(bufferSize, pageSize);
        File catalogFile = new File(databaseDir, "catalog.bin");
        catalog = new Catalog(catalogFile, pageSize);
        nextTempID = 0;
        this.pageSize = pageSize;
    }

    /**
     * Returns a name for a temporary table
     * @return The name of a temporary table
     */
    public String getTempTableName() {
        return String.valueOf(nextTempID++);
    }

    /**
     * Returns a specific Page from a table
     * @param tableName The name of the table to fetch the Page from
     * @param pageNumber The number of the Page being fetched
     * @return The fetched Page; `null` if no page exists with that number
     */
    public Page getPage(String tableName, int pageNumber) {
        return buffer.getPage(catalog.getTableSchema(tableName), pageNumber);
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
            currPage = buffer.getPage(tschema, pageNum);
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
            try {
                buffer.savePage(firstPage);
                buffer.insertPage(firstPage);
            } catch (IOException ioe) {
                System.err.println("Encountered exception while adding new page to table file: " + ioe.getMessage());
                return;
            }
        }
        // Iterate over pages until you find the one where the record goes
        int currIndex = 0;
        Page currPage = buffer.getPage(tschema, 0);
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
            currPage = buffer.getPage(tschema, currIndex);
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
                Page afterChild = buffer.getPage(tschema, currPage.pageNumber + 1);
                afterChild.prevPage = pageIndex;
                // Increment the page number for every page that follows child
                buffer.incrementPageNumbers(tableName, currPage.pageNumber);
            }
            // Insert the new page into the buffer
            try {
                buffer.insertPage(child);
                buffer.savePage(child);
            } catch (IOException ioe) {
                System.err.println("Failed to write split page to file. Error: " + ioe.getMessage());
            }
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
            currPage = buffer.getPage(tschema, pageNum);
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
            currPage = buffer.getPage(tschema, pageNum);
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
        catalog.createTableSchema(tableName, attributes);
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
     * Replaces one table with another. The target table's data will be dropped, and the source
     * table will be renamed to the target's name.
     * @param sourceName The name of the table whose data will be preserved
     * @param targetName The name of the table that will be replaced by the source table
     * @return `true` if the operation completed successfully; `false` otherwise
     */
    public boolean replaceTable(String sourceName, String targetName) {
        TableSchema sourceSchema = getTableSchema(sourceName);
        TableSchema targetSchema = getTableSchema(targetName);
        // Update the buffer, removing pages that belonged to the target and updating the schema for the source pages
        buffer.removeTable(targetName);
        buffer.updateSchema(sourceName, sourceSchema);  // TODO: Does this actually accomplish anything?
        // Update the schema in the catalog
        File oldSourceFile = sourceSchema.tableFile(); // This changes when you update the schema name
        sourceSchema.name = targetName;
        catalog.setTableSchema(targetName, sourceSchema);
        catalog.removeTableSchema(sourceName);
        // Verify both files exist before doing anything destructive
        File targetFile = targetSchema.tableFile();
        if (!targetFile.exists()) {
            System.err.println("Could not locate table file `" + targetFile.getAbsolutePath() + "`");
            return false;
        }
        if (!oldSourceFile.exists()) {
            System.err.println("Could not locate table file `" + oldSourceFile.getAbsolutePath() + "`");
            return false;
        }
        // Delete target file and rename source file
        if (!targetFile.delete()) {
            System.err.println("Failed to delete table file `" + targetFile.getAbsolutePath() + "`");
            return false;
        }
        if (!oldSourceFile.renameTo(targetFile)) {
            System.err.println("Failed to rename table file `" + oldSourceFile.getAbsolutePath() +
                    "` to `" + targetFile.getAbsolutePath() + "'");
        }
        return true;
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
            Page currPage = buffer.getPage(schema, 0);
            while (currPage != null) {
                totalRecords += currPage.recordCount();
                totalPages += 1;
                currPage = buffer.getPage(schema, totalPages);
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
        System.out.println("Buffer size: " + buffer.size());
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
     * @return The table's schema; `null` if table name does not exist in the catalog
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
        TableSchema schema = catalog.getTableSchema(tableName);
        File tableFile = schema.tableFile();
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

    public void test(ArrayList<String> args) {
        TableSchema table = catalog.getTableSchema(args.get(1));
        String cmd = args.get(2);
        String val = args.get(3);
        int num;
        Page page;
        switch (cmd) {
            case "save":
                num = Integer.parseInt(val);
                page = buffer.getPage(table, num);
                try {
                    buffer.savePage(page);
                } catch (IOException ioe) {
                    System.err.println("Failed to save page: " + ioe.getMessage());
                }
                break;
            case "load":
                num = Integer.parseInt(val);
                buffer.loadPage(table, num, num);
                break;
            case "print":
                num = Integer.parseInt(val);
                page = buffer.getPage(table, num);
                System.out.println(page);
                break;
            case "flush":
                try {
                    buffer.save();
                } catch (IOException ioe) {
                    System.err.println("Failed to save buffer: " + ioe.getMessage());
                }
                break;
        }
    }
}
