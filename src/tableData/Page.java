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
     * If the size of records exceeds the page size, a new page is created,
     * and the data is split
     * ONLY WORKS IS P# between (0, 127)
     */
    public void encodePage(){
        byte b_PageNum = (byte) pageNumber;
    }

    /**
     * Decodes the page given a chunk of binary in the format:
     * P#, Records, EOP
     */
    public void decodePage(){

    }
}
