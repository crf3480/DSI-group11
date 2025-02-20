package tableData;

import java.util.ArrayList;
import java.io.*;

/**
 * Represents a page of a Table
 * Authors:
 */
public class Page {

    public int pageNumber;
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
     * Encodes the given page to the end of the page file
     * Encodes to data to binary in the form P#, Records, EOP
     * ONLY WORKS IS P# between (0, 127) -> Increase page num to a long to fix
     * @param ts the table schema, used to get size of attr/record
     */
    public ByteArrayOutputStream encodePage(TableSchema ts) throws IOException {

        System.out.println("Encoding Page: " + pageNumber + " | Total Records: " + records.size());

        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        bs.write(pageNumber); // Writes the page number first as an int
        for (Record record : records) {
            bs.write(record.encodeRecord(ts).toByteArray()); // Encoded record
        }
        return bs;
    }

    /**
     * Decodes the page given a chunk of binary in the format:
     * P#, Records, EOP
     */
    public void decodePage(TableSchema ts){



    }

    private int getRecordSize(TableSchema ts) {
        int size = 0;
        for (Attribute attr : ts.attributes) {
            size += attr.length;
        }
        return size;
    }
}
