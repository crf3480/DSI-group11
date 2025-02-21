package components;

import org.w3c.dom.Attr;
import tableData.*;
import tableData.Record;
import utils.TestData;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

public class StorageManager {

    int bufferSize;
    HashMap<String, PageFileManager> buffer;
    Catalog catalog;

    int pageSize;

    public StorageManager(File databaseDir, int pageSize, int bufferSize) throws IOException {
        this.bufferSize = bufferSize;
        buffer = new HashMap<>();
        File catalogFile = new File(databaseDir, "catalog.bin");
        catalog = new Catalog(catalogFile, pageSize);
        this.pageSize = pageSize;
    }

    /**
     * gets record from a particular table by primary key
     * @param tableName name of table
     * @param key string of key to search
     * @return record with matching key (or null if no matches)
     */

    public PageFileManager getPageFileManager(String tableName) {
        PageFileManager pageManager = buffer.get(tableName);
        TableSchema tschema = catalog.getTableSchema(tableName);
        if (pageManager == null) {
            //this happens when the table is not in the buffer
            pageManager = new PageFileManager("./" + tableName + ".bin", pageSize, tschema);
            buffer.put(tableName, pageManager);
        }
        return pageManager;
    }
    public Record getByPrimaryKey(String tableName, String key)  {
        PageFileManager pageManager = getPageFileManager(tableName);
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
        for (Page p : pageManager.pages) {
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
        PageFileManager pageManager = buffer.get(tableName);
        TableSchema tschema = catalog.getTableSchema(tableName);
        if (pageManager == null) {
            //this happens when the table is not in the buffer
            pageManager = new PageFileManager("./" + tableName + ".bin", pageSize, tschema);
            buffer.put(tableName, pageManager);
        }
        for (Page p : pageManager.pages) {
            if (p.pageNumber == pageNum) {
                return p;
            }
        }

        return null;
    }

    public ArrayList<Record> getAllInTable(String tableName) {
        ArrayList<Record> records = new ArrayList<>();
        PageFileManager pageManager = getPageFileManager(tableName);
        for (Page p : pageManager.pages) {
            for (Record r : p.getRecords()) {
                    records.add(r);
                }
            }
        return records;
        }


    public boolean insertRecord(String tableName, ArrayList<String> values){
        return false;
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
        buffer.get(tableName).deletePageFile();
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
}
