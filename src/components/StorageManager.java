package components;

import tableData.*;
import tableData.Record;
import utils.TestData;

import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;

public class StorageManager {

    int bufferSize;
    Hashtable<String, ArrayList<Page>> buffer;
    Catalog catalog;

    public StorageManager(File databaseDir, int pageSize, int bufferSize) throws IOException {
        this.bufferSize = bufferSize;
        buffer = new Hashtable<>();
        File catalogFile = new File(databaseDir, "catalog.bin");
        catalog = new Catalog(catalogFile, pageSize);
    }

    public ArrayList<Record> getByPrimaryKey(int id){
        return null;
    }

    // Unsure about this one...
    public boolean pageByTableAndPageNum(int tableNum, int pageNum){
        return false;
    }

    public boolean getAllInTable(int tableNum){
        return false;
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

    public void createTable(String tableName, ArrayList<String> values) {
        catalog.addTableSchema(TestData.testTableSchema());
    }

    public void deleteTable(String tableName) {

    }

    public void displayTable(String tableName){
        System.out.println(catalog);
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
