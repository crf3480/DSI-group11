package components;

import tableData.Page;
import tableData.Record;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

// TBH Ive got some idea how this is going to look but don't have a complete idea due to how records are added.

public class PageFileManager {
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

    public PageFileManager(File dataFile, int pageSize) {
        this.dataFile = dataFile;
        this.pageSize = pageSize;

        // Check if page file exists
        if (dataFile.isFile()) {
            // If so: Read the content into pagefile manager (This functionality will be altered once
            // buffersize is not unlimited)
            try (FileInputStream fs = new FileInputStream(dataFile)) {
                //first byte of file is the # of pages
                int numPages = fs.read();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        else {
            // If not: Create blank datafile
        }
    }

    /**
     * Reads the .bin file where all the pages are
     */
    private void ParseDataFile(){

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
}
