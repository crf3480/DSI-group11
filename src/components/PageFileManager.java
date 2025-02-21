package components;

import tableData.Catalog;
import tableData.Page;
import tableData.Record;
import tableData.TableSchema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

// TBH Ive got some idea how this is going to look but don't have a complete idea due to how records are added.

public class PageFileManager {
    public TableSchema tableSchema;
    public ArrayList<Page> pages;
    public File dataFile;
    public ArrayList<tableData.Record> allRecords;
    public int pageSize;

    /**
     * Reads in a data file and initializes all the pages.
     * If there is no file it creates one
     * Populates all records
     * @param dataFile Name of data file (TableName.bin)
     */

    public PageFileManager(File dataFile, int pageSize, TableSchema tableSchema) {
        this.dataFile = dataFile;
        this.pageSize = pageSize;
        this.tableSchema = tableSchema;
        // Check if page file exists
        if (dataFile.isFile()) {
            // If so: Read the content into pagefile manager (This functionality will be altered once
            // buffersize is not unlimited
            ParseDataFile();
        }
        else {
            // If not: Create blank datafile
        }
    }

    /**
     * Reads the .bin file where all the pages are
     */
    private void ParseDataFile(){
        try (FileInputStream fs = new FileInputStream(dataFile)) {
            //first byte of file is the # of pages
            int numPages = fs.read();

            //reading each page into the pages arraylist
            for (int i = 0; i < numPages; ++i) {
                byte[] pageArr = fs.readNBytes(pageSize);
                Page pageToAdd = new Page(pageArr, this.tableSchema);
                this.pages.add(pageToAdd);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Inserts a single record to the records array
     * @param record single record to insert
     */
    public void InsertRecord(tableData.Record record){
        allRecords.add(record);
    }

    /**
     * Inserts an arraylist of records to the records array
     * @param records many records to the record list
     */
    public void InsertRecords(ArrayList<Record> records){
        allRecords.addAll(records);
    }


    /**
     * Saves the data of the page to the file
     * This is where the functionality of the records being split is
     */
    public void saveRecords(){
        // Figure out max records per page given
    }

    /**
     * Used to delete the table's file containing all the pages
     */
    public void deletePageFile() {
        try {
            //deleting the data file
            this.dataFile.delete();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
