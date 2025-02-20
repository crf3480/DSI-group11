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
     */
    public Page() {
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
        bs.write(records.size());
        for (Record record : records) {
            bs.write(record.encodeRecord(ts).toByteArray()); // Encoded record
        }

        return bs;
    }

    /**
     * Decodes a page starting with P#, NumRecords, Content
     * @param ts table schema for the page
     * @param pageBytes bytes for given page
     */
    public void decodePage(TableSchema ts, byte[] pageBytes) throws IOException {
        ByteArrayInputStream bs = new ByteArrayInputStream(pageBytes);
        DataInputStream ds = new DataInputStream(bs);

        this.pageNumber = ds.readInt();

        int recordCount = ds.readInt();
        int recordSize = getRecordSize(ts);

        for (int i = 0; i < recordCount; i++){
            byte[] singleRecord = ds.readNBytes(recordSize);
            Record record = new Record();
            record.decodeRecord(ts, singleRecord);
            records.add(record);
        }
    }

    /**
     * Gets the max size of a given record
     * @param ts table schema for the attribute list
     * @return size in bytes for the given record
     */
    private int getRecordSize(TableSchema ts) {
        int size = 0;
        for (Attribute attr : ts.attributes) {
            size += attr.length;
        }
        return size;
    }
}
