package tableData;

import java.util.ArrayList;

public class PageManager {
    public ArrayList<Page> pages;
    public String fileName;
    public ArrayList<Record> allRecords;
    public int pageSize;

    /**
     * Reads in a data file and initializes all the pages.
     * If there is no file it creates one
     * Populates all records
     * @param fileName Name of data file (TableName.bin)
     */
    public PageManager(String fileName, int pageSize) {
        this.fileName = fileName;
        this.pageSize = pageSize;

        // Check if pagefile exists

        // If so: Read the content

        // If not: Create blank datafile

    }

    /**
     * Reads the .bin file where all the pages are
     */
    private void ReadDataFile(){

    }

    /**
     * Inserts a single record to the records array
     * @param record single record to insert
     */
    public void InsertRecord(Record record){
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
     */
    public void save(){

    }
}
