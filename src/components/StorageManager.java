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
    }

    /**
     * Loads a table into the buffer
     * @param tableName The name of the table
     * @throws IOException
     */
    private void loadTable(String tableName) throws IOException {
        TableSchema tschema = catalog.getTableSchema(tableName);
        ArrayList<Page> pageList = ParseDataFile(catalog.getFilePath().getParent() + File.separator + tableName + ".bin", tschema);
        buffer.put(tableName, pageList);
    }

    /**
     * Returns the list of pages of a specified table
     * @param tableName The name of the table
     * @return An ArrayList of every Page in that table. If table does not exist, `null`
     */
    public ArrayList<Page> getPageList(String tableName) {
        ArrayList<Page> pageManager = buffer.get(tableName);
        if (pageManager == null) {
            try {
                loadTable(tableName);
            } catch (IOException e) {
                System.err.println("Table not found: `" + tableName + "`");
                return null;
            }
        }
        return pageManager;
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
                if (r.rowData.get(primIndex).equals(key)) {
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
            System.err.println("Table `" + tableName + "` not found");
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
     * @param tableName
     * @param values
     */
    public void insertRecord(String tableName, ArrayList<ArrayList<Object>> values){
        //Step 1: get the pages for that table
        ArrayList<Page> pages = getPageList(tableName);
        //Step 2: loop through the table's pages, and try to insert at each one.
        int valuesIndex = 0;
        boolean ranThroughOnce = false;
        Page prevPage = null;
        while (valuesIndex != values.size()) {
            //this top branch runs if all the pages are full but there are still records to be inserted
            if (ranThroughOnce) {
                //splitting shit
                Page split = prevPage.split();
                pages.add(split);
            }
            ranThroughOnce = true;
            //loops through the table's pages and tries to insert at each one, one by one
            if (pages.size() == 0) {
                pages.add(new Page(1, catalog.getTableSchema(tableName), this.pageSize));
            }
            for (Page p : pages) {
                prevPage = p;
                for (int i = valuesIndex; i < values.size(); ++i) {
                    if (!p.insertRecord(new Record(values.get(i)))) {
                        break;
                    }
                    ++valuesIndex;
                }
            }
        }
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
                if (r.rowData.get(primIndex).equals(key)) {
                    r.rowData.remove(primIndex);
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
                if (r.rowData.get(primIndex).equals(key)) {
                    r.rowData.remove(primIndex);
                    r.rowData.add(primIndex, record);
                }
            }
        }
        return false;
    }

    public void createTable(String tableName, ArrayList<Attribute> values) {
        catalog.addTableSchema(new TableSchema(tableName, values));
        buffer.put(tableName, new ArrayList<Page>());
    }

    public void deleteTable(String tableName) {
        this.catalog.removeTableSchema(tableName);
        File dataFile = new File(this.catalog.getFilePath() + File.separator + tableName + ".bin");
        dataFile.delete();
    }

    public boolean addAttribute(String tableName, Attribute newAttribute) {
        ArrayList<Page> pageList = getPageList(tableName);
        if (!buffer.containsKey(tableName)) {
            System.err.println("Table " + tableName + " does not exist");
            return false;
        }
        TableSchema schema = catalog.getTableSchema(tableName);
        schema.attributes.add(newAttribute);
        for (Page p : pageList) {
            p.updateSchema(schema);
        }
        return true;
    }

    /**
     * Removes an attribute from a given table
     * @param tableName The name of the table to remove the attribute from
     * @param attributeName The name of the attribute to remove. Should be
     * @return `true` if the attribute was successfully dropped; `false` if there was an error
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
            System.err.println("Table " + tableName + " has no attribute '" + attributeName + "'");
            return false;
        }
        schema.attributes.remove(attrIndex);
        for (Page p : pageList) {
            p.updateSchema(schema);
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
            System.err.println("Table not found: `" + tableName + "`");
            return;
        }
        try {
            loadTable(tableName);  // Load table into memory
        } catch (IOException e) {
            System.err.println("Failed to load table `" + tableName + "`");
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
        System.out.println("\nTables:\n");
        for(String table : catalog.getTableNames()) {
            try {
                loadTable(table);
            } catch (IOException e) {
                System.err.println("Failed to load table `" + table + "`");
                return;
            }
            displayTable(table);
        }
    }

    public TableSchema getTableSchema(String tableName){
        return catalog.getTableSchema(tableName);
    }

    /**
     * Writes the buffer and catalog to disk
     * @return `true` if this operation succeeded; `false` if there was an error
     */
    public boolean save() {
        try {
            catalog.save();
        } catch (IOException e) {
            System.err.println("ERROR: Failed to save catalog to disk: " + e.getMessage());
        }
        for (String tableName : buffer.keySet()) {
            ArrayList<Page> pages = buffer.get(tableName);
            File tableFile = new File(catalog.getFilePath().getParent() + File.separator + tableName + ".bin");
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
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    private ArrayList<Page> ParseDataFile(String dataFile, TableSchema tableSchema){
        ArrayList<Page> pages = new ArrayList<>();
        try (FileInputStream fs = new FileInputStream(dataFile)) {
            try (DataInputStream in = new DataInputStream(fs)) {
                //first byte of file is the # of pages
                int numPages = in.readInt();
                //reading each page into the pages arraylist
                byte[] pageBytes = new byte[pageSize];
                for (int i = 0; i < numPages; ++i) {
                    in.read(pageBytes);
                    Page pageToAdd = new Page(pageBytes, tableSchema);
                    pages.add(pageToAdd);
                }
            } catch (Exception e) {
                System.err.println("Error while reading table data file: " + e.getMessage());
            }

        }
        catch (FileNotFoundException e) {
            throw new RuntimeException("Table file `" + dataFile + "` not found.");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return pages;
    }
}
