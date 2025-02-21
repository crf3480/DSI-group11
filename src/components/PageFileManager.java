package components;

import tableData.Catalog;
import tableData.Page;
import tableData.Record;
import tableData.TableSchema;

import java.io.*;
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
     * @param dataFilePath Name of data file (TableName.bin)
     */

    public PageFileManager(String dataFilePath, int pageSize, TableSchema tableSchema) {
        this.dataFile = new File(dataFilePath + "/" + tableSchema.name + ".bin");
        this.pageSize = pageSize;
        this.tableSchema = tableSchema;
        pages = new ArrayList<>();
        // Check if page file exists
        if (dataFile.isFile()) {
            // If so: Read the content into pagefile manager (This functionality will be altered once
            // buffersize is not unlimited
            System.out.println("Loading data from " + dataFile.getAbsolutePath());
            ParseDataFile();
        }
        else {
            try {
                dataFile.createNewFile();
            } catch (IOException e) {

            }
        }
    }

    /**
     * Reads the .bin file where all the pages are
     */
    private void ParseDataFile(){

        try (FileInputStream fs = new FileInputStream(dataFile)) {
            try (DataInputStream dis = new DataInputStream(fs)) {
                //first byte of file is the # of pages
                int numPages = dis.readInt();
                System.out.println("Number of pages: " + numPages);
                byte[] pageArr = new byte[pageSize];
                // reading each page into the pages arraylist
                for (int i = 0; i < numPages; ++i) {
                    int readBytes = dis.read(pageArr);
                    System.out.println("Read " + readBytes + " bytes");
                    Page pageToAdd = new Page(pageArr, this.tableSchema);
                    System.out.println("Adding page: " + pageToAdd);
                    pages.add(pageToAdd);
                }
            } catch (Exception e) {
                System.out.println("Error while reading in pages: " + e.getMessage());
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
     * Manually inserts pages into the page manager for testing
     * @param page The page to insert
     */
    public void insertPage(Page page){
        pages.add(page);
    }

    /**
     * Saves the data of the page to the file
     */
    public void saveRecords() throws IOException {
        FileOutputStream outStream = new FileOutputStream(dataFile);
        DataOutputStream out = new DataOutputStream(outStream);
        out.writeInt(pages.size());
        for (Page page : pages) {
            byte[] encodedPage = page.encodePage();
            System.out.println(encodedPage.length);
            out.write(encodedPage);
        }
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
