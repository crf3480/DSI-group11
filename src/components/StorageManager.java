package components;

import tableData.*;
import tableData.Record;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * StorageManager.java
 * Manages fetching and saving pages to file
 */
public class StorageManager {

    int bufferSize;
    HashMap<String, ArrayList<Page>> buffer;
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
        buffer = new HashMap<>();
        File catalogFile = new File(databaseDir, "catalog.bin");
        catalog = new Catalog(catalogFile, pageSize);
        this.pageSize = pageSize;
        for (String name : catalog.getTableNames()) {
            loadTable(name);
        }
    }

    /**
     * Loads a table into the buffer
     * @param tableName The name of the table
     * @throws IOException An error was encountered when parsing the data file
     */
    private void loadTable(String tableName) throws IOException {
        TableSchema tschema = catalog.getTableSchema(tableName);
        ArrayList<Page> pageList = ParseDataFile(catalog.getTableFile(tableName), tschema);
        if (!buffer.containsKey(tableName)) {
            buffer.put(tableName, pageList);
        }
    }

    /**
     * Returns the list of pages of a specified table
     * @param tableName The name of the table
     * @return An ArrayList of every Page in that table. If table does not exist, 'null'
     */
    public ArrayList<Page> getPageList(String tableName) {
        //This might be useful in the future, but right now the buffer always contains the list of pages
//        ArrayList<Page> pageManager = buffer.get(tableName);
//        if (pageManager == null) {
//            try {
//                loadTable(tableName);
//            } catch (IOException e) {
//                System.err.println("Table not found: '" + tableName + "'");
//                return null;
//            }
//        }
        return buffer.get(tableName);
    }

    /**
     * gets record from a particular table by primary key
     * @param tableName name of table
     * @param key string of key to search
     * @return record with matching key (or null if no matches)
     */
    public Record getByPrimaryKey(String tableName, String key)  {
        ArrayList<Page> pageManager = getPageList(tableName);
        TableSchema tschema = catalog.getTableSchema(tableName);
        //finding the attribute index with the primary key
        int primIndex = 0;
        for (Attribute a : tschema.attributes) {
            if (a.primaryKey) {
                break;
            }
            primIndex++;
        }
        //looping through pages and records to find The One
        for (Page p : pageManager) {
            for (Record r : p.getRecords()) {
                if (r.get(primIndex).equals(key)) {
                    return r;
                }
            }
        }
        return null;
    }

    // Unsure about this one...
    public Page pageByTableAndPageNum(String tableName, int pageNum){
        ArrayList<Page> pageManager = getPageList(tableName);
        for (Page p : pageManager) {
            if (p.pageNumber == pageNum) {
                return p;
            }
        }

        return null;
    }

    public ArrayList<Record> getAllInTable(String tableName) {
        getPageList(tableName);
        if (buffer.get(tableName) == null) {
            System.err.println("Table '" + tableName + "' does not exist.");
            return null;
        }
        ArrayList<Record> records = new ArrayList<>();
        ArrayList<Page> pageManager = getPageList(tableName);
        for (Page p : pageManager) {
            for (Record r : p.getRecords()) {
                    records.add(r);
                }
            }
        return records;
        }

    /**
     * Inserts a record into a given table
     * @param tableName The name of the table to insert the records into
     * @param record The record to insert
     */
    public void insertRecord(String tableName, Record record) {
        // Get the pages for that table
        ArrayList<Page> pages = getPageList(tableName);
        TableSchema tschema = catalog.getTableSchema(tableName);
        if (tschema == null) {
            throw new IllegalArgumentException("Table '" + tableName + "' does not exist.");
        }
        // If table is empty, insert a new page
        if (pages.isEmpty()) {
            pages.add(new Page(1, tschema, this.pageSize));
        }
        // Attempt to insert the record into each page
        for (Page p : pages) {
            if (p.insertRecord(record)) {
                return;
            }
        }
        // If that did not work, split the first page and try again
        // Increment page numbers before the split
        for (Page p : pages) {
            if (p.pageNumber > pages.getLast().pageNumber + 1){
                p.pageNumber++;
            }
        }
        Page split = pages.getLast().split();
        pages.add(split);
        save();
    }

    public boolean deleteByPrimaryKey(String tableName, String key){
        ArrayList<Page> pageManager = getPageList(tableName);
        TableSchema tschema = catalog.getTableSchema(tableName);
        //finding the attribute index with the primary key
        int primIndex = 0;
        for (Attribute a : tschema.attributes) {
            if (a.primaryKey) {
                break;
            }
            primIndex++;
        }
        for (Page p : pageManager) {
            for (Record r : p.getRecords()) {
                if (r.get(primIndex).equals(key)) {
                    r.rowData.remove(primIndex);  // <-- What's the reason to do this?
                    //Need to actually delete the record
                }
            }
        }
        return false;
    }

    public boolean updateByPrimaryKey(String tableName, String key, Record record){
        ArrayList<Page> pageManager = getPageList(tableName);
        TableSchema tschema = catalog.getTableSchema(tableName);
        //finding the attribute index with the primary key
        int primIndex = 0;
        for (Attribute a : tschema.attributes) {
            if (a.primaryKey) {
                break;
            }
            primIndex++;
        }
        for (Page p : pageManager) {
            for (Record r : p.getRecords()) {
                if (r.get(primIndex).equals(key)) {
                    r.update(primIndex, record);
                }
            }
        }
        return false;
    }

    /**
     * Creates a table with a given name in the catalog and creates a file for it
     * @param tableName The name of the table
     * @param attributes The list of attributes in each record of the table
     * @throws IOException If an error is encountered when creating the table file
     */
    public void createTable(String tableName, ArrayList<Attribute> attributes) throws IOException {
        if (catalog.addTableSchema(new TableSchema(tableName, attributes))) {
            buffer.put(tableName, new ArrayList<>());
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
        buffer.remove(tableName);
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
        if (newAttribute.primaryKey) {
            System.err.println("Cannot add primary key to an existing table.");
            return false;
        }
        if (newAttribute.notNull && newAttribute.defaultValue == null) {
            System.err.println("New attribute is 'notnull', but 'null' is the default value.");
            return false;
        }
        ArrayList<Page> pageList = getPageList(tableName);
        if (!buffer.containsKey(tableName)) {
            System.err.println("Table " + tableName + " does not exist");
            return false;
        }
        TableSchema schema = catalog.getTableSchema(tableName);
        for (Attribute a : schema.attributes) {
            if (a.name.equals(newAttribute.name)) {
                System.err.println("Table " + tableName + " already contains attribute '" + newAttribute.name + "'.");
                return false;
            }
        }
        for (Page p : pageList) {
            p.updateSchema(schema);
        }
        schema.attributes.add(newAttribute);
        catalog.setTableSchema(tableName, schema);
        System.out.println("Added attribute '" + newAttribute.name + "' to table '" + tableName + "'");
        return true;
    }

    /**
     * Removes an attribute from a given table, so long as the attribute is not the primary key
     * @param tableName The name of the table to remove the attribute from
     * @param attributeName The name of the attribute to remove. Should be
     * @return 'true' if the attribute was successfully dropped; 'false' if there was an error
     */
    public boolean dropAttribute(String tableName, String attributeName) {
        ArrayList<Page> pageList = getPageList(tableName);
        if (!buffer.containsKey(tableName)) {
            System.err.println("Table " + tableName + " does not exist");
            return false;
        }
        TableSchema schema = catalog.getTableSchema(tableName);
        ArrayList<Attribute> attributes = schema.attributes;
        int attrIndex = schema.getAttributeIndex(attributeName);
        if (attrIndex == -1) {
            System.err.println("Cannot drop attribute '" + attributeName + "': no such attribute.");
            return false;
        }
        if (attributes.get(attrIndex).primaryKey) {
            System.err.println("Cannot drop attribute '" + attributeName + "': key is primary key.");
            return false;
        }
        schema.attributes.remove(attrIndex);
        for (Page p : pageList) {
            p.updateSchema(schema);
        }
        catalog.setTableSchema(tableName, schema);
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
        System.out.println("Pages: " + buffer.get(tableName).size());
        int totalPages = 0;
        for (Page p : buffer.get(tableName)) {
            totalPages += p.getRecords().size();
        }
        System.out.println("Records: " + totalPages);
    }

    /**
     * Displays the schema of the database to the console
     * <ul>
     *     <li>Database location</li>
     *     <li>Page size</li>
     *     <li>Buffer size</li>
     *     <li>Table schema</li>
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
                try {
                    loadTable(table);
                } catch (IOException e) {
                    System.err.println("Failed to load table '" + table + "'");
                    return;
                }
                displayTable(table);
            }
        }
    }

    public TableSchema getTableSchema(String tableName){
        return catalog.getTableSchema(tableName);
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
        for (String tableName : buffer.keySet()) {
            if (!save(tableName)) {  // Try saving each table to file
                return false;
            }
        }
        return false;
    }

    /**
     * Writes the buffer for a particular table to file
     * @return 'true' if this operation succeeded; 'false' if there was an error
     */
    public boolean save(String tableName) {
        ArrayList<Page> pages = buffer.get(tableName);
        if (pages == null) {
            System.err.println("Table " + tableName + " does not exist");
            return false;
        }
        File tableFile = catalog.getTableFile(tableName);
        try (FileOutputStream fs = new FileOutputStream(tableFile)) {
            try (DataOutputStream dis = new DataOutputStream(fs)) {
                // First value of file is the # of pages
                dis.writeInt(pages.size());
                // Write each page
                for (Page page : pages) {
                    dis.write(page.encodePage());
                }
            } catch (Exception e) {
                System.out.println("Error while reading in pages: " + e.getMessage());
                return false;
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    /**
     * Parses a table file into a sequence of pages using a given schema
     * @param dataFile A File object pointing to the table's data file
     * @param tableSchema The schema of the table data
     * @return The list of Page objects that were stored in the table file
     */
    private ArrayList<Page> ParseDataFile(File dataFile, TableSchema tableSchema) throws IOException {
        ArrayList<Page> pages = new ArrayList<>();
        try (FileInputStream fs = new FileInputStream(dataFile)) {
            try (DataInputStream in = new DataInputStream(fs)) {
                //first byte of file is the # of pages
                int numPages = in.readInt();
                //reading each page into the pages arraylist
                byte[] pageBytes = new byte[pageSize];
                for (int i = 0; i < numPages; ++i) {
                    int bytesRead = in.read(pageBytes);
                    if (bytesRead != pageSize) {
                        throw new IOException("Encountered EOF while reading page " + i + "; expected "
                                + pageSize + " but got " + bytesRead);
                    }
                    Page pageToAdd = new Page(pageBytes, tableSchema);
                    pages.add(pageToAdd);
                }
            } catch (Exception e) {
                System.err.println("Error while reading table data file: " + e.getMessage());
            }
        }
        catch (FileNotFoundException e) {
            throw new IOException("Table file '" + dataFile + "' not found.");
        }
        catch (IOException e) {
            throw e;  // Propagate IO exceptions up
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return pages;
    }
}
