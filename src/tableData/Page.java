package tableData;

import java.util.ArrayList;
import java.io.*;

/**
 * Represents a page of a Table
 * Authors:
 */
public class Page {

    public int pageNumber;  // Increase this to a long if you need more than 127 pages
    public ArrayList<Record> records;

    /**
     * Creates a page object, given the page number and records
     * @param pageNumber Number of given page
     */
    public Page(int pageNumber, ArrayList<Record> records) {
        this.pageNumber = pageNumber;
        this.records = records;
    }

    /**
     * Creates a page object given page number, expecting the records to be populated in decode method
     * @param pageNumber page num of given page
     */
    public Page(int pageNumber) {
        this.pageNumber = pageNumber;
        this.records = new ArrayList<>();
    }

    /**
     * Used in page splitting in order to add a new page number to a given page
     * @param newPageNumber to be added
     */
    public void UpdatePageNumber(int newPageNumber) {
        this.pageNumber = newPageNumber;
    }

    /**
     * Encodes the given page to the end of the page file
     * Encodes to data to binary in the form P#, Records, EOP
     * ONLY WORKS IS P# between (0, 127)
     * @param ts the table schema, used to get size of attr/record
     */
    public ByteArrayOutputStream encodePage(TableSchema ts) throws IOException {

        System.out.println("Encoding Page: " + pageNumber + " | Total Records: " + records.size());

        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        bs.write(pageNumber); // Writes the page number first as an int
        for (Record record : records) {
            bs.write(record.encodeRecord(ts).toByteArray()); // Encoded record
        }

        // TODO: Do something so signal all records have been encoded

        return bs;
    }

    /**
     * Decodes the page given a chunk of binary in the format:
     * P#, Records, EOP
     */
    public void decodePage(TableSchema ts, byte[] pageBytes) throws IOException {
        ByteArrayInputStream bs = new ByteArrayInputStream(pageBytes);
        DataInputStream ds = new DataInputStream(bs);

        pageNumber = ds.readInt();

        int recordSize = getRecordSize(ts);

        // TODO: read until you hit the end condition
        while (true){ // Reads infinitely, change this once we have a set end condition
            byte[] singleRecord = ds.readNBytes(recordSize);
            Record record = new Record();
            record.decodeRecord(ts, singleRecord);
            records.add(record);
        }
    }

    private int getRecordSize(TableSchema ts) {
        int size = 0;
        for (Attribute attr : ts.attributes) {
            size += attr.length;
        }
        return size;
    }
}
