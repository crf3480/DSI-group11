package components;
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
     * @throws IOException If there are problems accessing or modifying the catalog and table files
     */
    public StorageManager(File databaseDir, int pageSize, int bufferSize) throws IOException {
        File catalogFile = new File(databaseDir, "catalog.bin");
        catalog = new Catalog(catalogFile, pageSize);
        buffer = new Buffer(bufferSize, catalog.pageSize());
        wipeTempTables();
        nextTempID = 0;
    }

    /**
     * Get the page size for the database
     * @return The database's page size
     */
    public int pageSize() {
        return catalog.pageSize();
    }

    /**
     * Finds and deletes all tables in the database that start with a numeric character
     * This is mostly used for crash recovery, since all temp tables are supposed to be deleted after use.
     * In the event that the program closes/crashes before it can delete these, this will avoid the
     * program crashing upon the first attempt to create a temp table on next run
     * Standard table files cannot have names starting with a number, so this will only delete temp tables.
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
    }

    /**
     * Toggles "NUKE MODE", which deletes all files in the DB directory upon exit
     */
    public void toggleNUKE_MODE() {
        NUKE_MODE = !NUKE_MODE;

        System.err.println("\nNUKE MODE "+((NUKE_MODE)? "enabled. Entire database will be deleted on program close.\n" : "disabled. Database will be saved as usual.\n"));
        try {
            TimeUnit.MILLISECONDS.sleep(50);
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

    /**
     * Returns a name for a temporary table
     * @return The name of a temporary table
     */
    public String getTempTableName() {
        return String.valueOf(nextTempID++);
    }

    /**
     * Returns a specific Page from a table
     * @param schema The TableSchema of the table the Page belongs to
     * @param pageNumber The number of the Page being fetched
     * @return The fetched Page; `null` if no page exists with that number
     */
    public Page getPage(TableSchema schema, int pageNumber) {
        return buffer.getPage(schema, pageNumber);
    }

    /**
     * Inserts a record into the table, ordered by the specified attribute
     * @param schema The TableSchema of the table the record is being inserted into
     * @param record The record to insert
     * @param attrIndex The index of the attribute the table will be sorted by
     * @return `true` if the record was successfully inserted; `false` otherwise
     */
    public boolean insertRecord(TableSchema schema, Record record, int attrIndex) {
        // If table has no pages, make a new page and insert it into the buffer
        if (schema.rootIndex == -1) {
            Page firstPage = new Page(0, 0, schema, catalog.pageSize());
            try {
                buffer.savePage(firstPage);
                buffer.insertPage(firstPage);
            } catch (IOException ioe) {
                System.err.println("Encountered exception while adding new page to table file: " + ioe.getMessage());
                return false;
            }
        }
        // Verify record is unique. While looping, find and remember the insertion point
        int targetPageNum = -1;
        int targetRecordIndex = -1;
        int pageIndex = 0;
        Page currPage = schema.rootIndex == -1 ? null : getPage(schema, pageIndex);
        while (currPage != null) {
            for (int i = 0; i < currPage.recordCount(); i++) {
                Record existingRec = currPage.records.get(i);
                // Check for duplicate
                int matchAttr = record.isEquivalent(existingRec, schema);
                if (matchAttr != -1) {
                    System.err.println("Invalid new tuple: the value '" + record.get(matchAttr) +
                            "' already exists in "+ ((schema.attributes.get(matchAttr).primaryKey ? "primary key " : "unique ")
                            +"column '" + schema.attributes.get(matchAttr).name + "'."));
                    return false;
                }
                // Check for insertion point
                if (targetPageNum == -1 && !record.greaterThan(existingRec, schema, attrIndex)) {
                    targetPageNum = pageIndex;
                    targetRecordIndex = i;
                }
            }
            pageIndex += 1;
            currPage = getPage(schema, pageIndex);
        }
        // If targetPageNum was never updated, record goes at the end of the table
        if (targetPageNum == -1) {
            targetPageNum = schema.pageCount() - 1;

        }
        // Insert record into target page/index
        Page targetPage = getPage(schema, targetPageNum);
        if (targetRecordIndex == -1) {
            targetPage.records.add(record);
        } else {
            targetPage.records.add(targetRecordIndex, record);
        }
        schema.incrementRecordCount();

        // If the page is now oversize, split
        if (targetPage.pageDataSize() > catalog.pageSize()) {
            try {
                pageIndex = expandTable(schema);
            } catch (IOException e) {
                // If there was a failure, undo the record insert and abort
                System.err.println(e.getMessage());
                targetPage.records.remove(targetRecordIndex);
                return false;
            }
            Page afterChild = buffer.getPage(schema, targetPage.pageNumber + 1); // Get this BEFORE you mess with root's nextIndex
            Page child = targetPage.split(pageIndex);
            assert (child.nextPage == -1) == (afterChild == null);
            if (child.nextPage != -1) {
                // Update the prevPage pointer for the page after this one, if one exists
                afterChild.prevPage = pageIndex;
                // Increment the page number for every page that follows child
                buffer.incrementPageNumbers(schema, child.pageNumber);
            }
            // Insert the new page into the buffer
            try {
                buffer.insertPage(child);
                buffer.savePage(child);
            } catch (IOException ioe) {
                System.err.println("Failed to write split page to file. Error: " + ioe.getMessage());
            }
        }
        return true;
    }

    /**
     * Inserts a record at the end of the table, regardless of key ordering
     * @param schema The TableSchema of the table the record is being inserted into
     * @param record The record to insert
     */
    public void fastInsert(TableSchema schema, Record record) {
        // If table has no pages, make a new page and insert it into the buffer
        if (schema.rootIndex == -1) {
            Page firstPage = new Page(0, 0, schema, catalog.pageSize());
            try {
                buffer.savePage(firstPage);
                buffer.insertPage(firstPage);
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
                pageIndex = expandTable(schema);
            } catch (IOException e) {
                System.err.println(e.getMessage());
                return;
            }
            // Create Page with new record and link it with prev
            ArrayList<Record> recordList = new ArrayList<>();
            recordList.add(record);
            Page newPage = new Page(pageIndex, lastPage.pageNumber + 1, recordList, pageSize(), schema);
            newPage.prevPage = lastPage.pageIndex;
            lastPage.nextPage = newPage.pageIndex;
            // Insert the new page into the buffer
            try {
                buffer.insertPage(newPage);
                buffer.savePage(newPage);
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
        int pageNum = page.pageNumber;
        // Remove page
        buffer.removePage(page);
        schema.decrementPageCount();
        // Link adjacent pages
        Page prevPage = getPage(schema, pageNum - 1);
        Page nextPage = getPage(schema, pageNum + 1);
        // If prev page exists, set its next, otherwise make the next page root
        if (prevPage != null) {
            prevPage.nextPage = page.nextPage;
        } else {
            schema.rootIndex = page.nextPage;
        }
        // If next page exists, set its prev page (if next page is null, do nothing)
        if (nextPage != null) {
            nextPage.prevPage = page.prevPage;
        }
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
     * @return The table's schema; `null` if table name does not exist in the catalog
     */
    public TableSchema getTableSchema(String tableName){
        return catalog.getTableSchema(tableName);
    }

    /**
     * Increases the size of a table file by a single page. Returns the index of the page
     * that would occupy the added space
     * @param schema The TableSchema for the table being expanded
     * @return The index of the added page
     */
    private int expandTable(TableSchema schema) throws IOException {
        File tableFile = schema.tableFile();
        int newIndex = (int) tableFile.length() / catalog.pageSize();  // Calculate index before expanding table
        try (RandomAccessFile out = new RandomAccessFile(tableFile, "rw")) {
            out.seek(tableFile.length());
            out.write(new byte[catalog.pageSize()]);
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
}
