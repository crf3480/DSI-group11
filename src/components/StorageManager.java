package components;

import tableData.*;
import tableData.Record;
import utils.TestData;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class StorageManager {

    int bufferSize;
    HashMap<String, ArrayList<Page>> buffer;
    Catalog catalog;

    int pageSize;

    public StorageManager(File databaseDir, int pageSize, int bufferSize) throws IOException {
        this.bufferSize = bufferSize;
        buffer = new HashMap<>();
        File catalogFile = new File(databaseDir, "catalog.bin");
        catalog = new Catalog(catalogFile, pageSize);
        this.pageSize = pageSize;
    }

    public ArrayList<Page> getPageFileManager(String tableName) {
        ArrayList<Page> pageManager = buffer.get(tableName);
        TableSchema tschema = catalog.getTableSchema(tableName);
        if (pageManager == null) {
            //this happens when the table is not in the buffer
            pageManager = ParseDataFile("./" + tableName + ".bin", tschema);
            buffer.put(tableName, pageManager);
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
        ArrayList<Page> pageManager = getPageFileManager(tableName);
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
        ArrayList<Page> pageManager = getPageFileManager(tableName);
        for (Page p : pageManager) {
            if (p.pageNumber == pageNum) {
                return p;
            }
        }

        return null;
    }

    public ArrayList<Record> getAllInTable(String tableName) {
        ArrayList<Record> records = new ArrayList<>();
        ArrayList<Page> pageManager = getPageFileManager(tableName);
        for (Page p : pageManager) {
            for (Record r : p.getRecords()) {
                    records.add(r);
                }
            }
        return records;
        }


    public void insertRecord(String tableName, ArrayList<ArrayList<Object>> values){
        //Step 1: get the pages for that table
        ArrayList<Page> pages = getPageFileManager(tableName);
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
            for (Page p : pages) {
                prevPage = p;
                for (int i = valuesIndex; i < values.size(); ++i) {
                    if (!p.insertRecord(new Record(values.get(i)))) {
                        break;
                    }
                }
            }
        }
    }

    public boolean deleteByPrimaryKey(int id){
        return false;
    }

    public boolean updateByPrimaryKey(int id){
        return false;
    }

    public void createTable(String tableName, ArrayList<Attribute> values) {
        catalog.addTableSchema(TestData.testTableSchema(7));
    }

    public void deleteTable(String tableName) {
        //TODO: DELETE TABLE FROM SCHEMA TOO
        File dataFile = new File("./" + tableName + ".bin");
        dataFile.delete();
    }

    public boolean addAttribute(String tableName, Attribute newAttribute) {
        return false;
    }

    public boolean deleteAttribute(String tableName, String attributeName) {
        return false;
    }

    public void displayTable(String tableName){
        System.out.println(catalog);
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
            return true;
        } catch (IOException e) {
            System.err.println("ERROR: Failed to save catalog to disk: " + e.getMessage());
        }
        return false;
    }

    private ArrayList<Page> ParseDataFile(String dataFile, TableSchema tableSchema){
        ArrayList<Page> pages = new ArrayList<>();
        try (FileInputStream fs = new FileInputStream(dataFile)) {
            //first byte of file is the # of pages
            int numPages = fs.read();

            //reading each page into the pages arraylist
            for (int i = 0; i < numPages; ++i) {
                byte[] pageArr = fs.readNBytes(pageSize);
                Page pageToAdd = new Page(pageArr, tableSchema);
                pages.add(pageToAdd);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return pages;
    }
}
