package tableData;

import java.util.ArrayList;
import java.io.*;
import java.util.Arrays;
import java.util.Collection;

/**
 * Represents a page of a Table
 * Authors:
 */
public class Page {

    private TableSchema tableSchema;
    public int pageNumber;
    public byte[] recordData;
    private int numRecords;

    /**
     * Creates a page object from a Page data byte array
     * @param pageData The byte array of page data
     * @param tableSchema The schema of the data in this page
     */
    public Page(byte[] pageData, TableSchema tableSchema) throws IOException {
        this.tableSchema = tableSchema;

        ByteArrayInputStream inStream = new ByteArrayInputStream(pageData);
        DataInputStream in = new DataInputStream(inStream);
        pageNumber = in.readInt();
        numRecords = in.readInt();
        recordData = new byte[pageData.length - (Integer.SIZE * 2)];
        in.readFully(recordData);  // Read page data into the recordData array
    }

    /**
     * Creates an empty page with a given page number
     * @param pageNumber The number of the page
     * @param tableSchema The schema of the records stored in this page
     * @param pageSize The page size in bytes
     */
    public Page(int pageNumber, TableSchema tableSchema, int pageSize) {
        this.pageNumber = pageNumber;
        this.tableSchema = tableSchema;
        recordData = new byte[pageSize - (Integer.BYTES * 2)];  // Leave room for the page # and record count
        numRecords = 0;
    }

    /**
     * Inserts a record into the page
     * @param record The record being inserted into the page
     * @return `true` if the record was inserted. `false` if the record could not be inserted because it is full
     * @throws IOException Encountered a problem when converting records to or from binary
     */
    public boolean insertRecord(Record record) throws IOException {
        ArrayList<Record> records = getRecords();
        records.add(record);
        numRecords += 1;
        byte[] encoded = encodeRecords(records);
        if (encoded.length > recordData.length) {
            return false;
        } else if (encoded.length < recordData.length) {
            throw new IOException("Invalid page size found when encoding records");
        }
        recordData = encoded;
        return true;
    }

    /**
     * Used in page splitting in order to add a new page number to a given page
     * @param newPageNumber to be added
     */
    public void updatePageNumber(int newPageNumber) {
        this.pageNumber = newPageNumber;
    }

    /**
     * Gets the list of records in the page
     * @return The list of records stored in the page
     */
    public ArrayList<Record> getRecords() throws IOException {
        ArrayList<Record> records = new ArrayList<>(numRecords);
        ByteArrayInputStream inStream = new ByteArrayInputStream(recordData);
        DataInputStream in = new DataInputStream(inStream);
        for (int i = 0; i < numRecords; i++) {
            ArrayList<Object> recordAttr = new ArrayList<>();
            for (Attribute attr : tableSchema.attributes) {
                switch (attr.type) {
                    case INT:
                        recordAttr.add(in.readInt());
                        break;
                    case DOUBLE:
                        recordAttr.add(in.readDouble());
                        break;
                    case BOOLEAN:
                        recordAttr.add(in.readBoolean());
                    case CHAR:
                    case VARCHAR:
                        recordAttr.add(in.readUTF());
                        break;
                    default:
                        throw new IOException("Invalid attribute type: " + attr.type);
                }
            }
            records.add(new Record(recordAttr));
        }
        return records;
    }

    /**
     * Converts a collection of records into an array of bytes. This array will be at minimum the size of the page,
     * but may be longer if the list of records cannot fit in the page.
     * @param records The records being encoded
     * @return An array of bytes containing the data of the records
     * @throws IOException If there is an error encoding the data
     */
    private byte[] encodeRecords(Collection<Record> records) throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream(recordData.length);
        DataOutputStream out = new DataOutputStream(outStream);
        for (Record record : records) {
            for (int i = 0; i < tableSchema.attributes.size(); i++) {
                Attribute attr = tableSchema.attributes.get(i);
                Object value = record.rowData.get(i);
                switch (attr.type) {
                    case INT:
                        out.writeInt((Integer) value);
                        break;
                    case DOUBLE:
                        out.writeDouble((Double) value);
                        break;
                    case BOOLEAN:
                        out.writeBoolean((Boolean) value);
                        break;
                    case CHAR:
                    case VARCHAR:
                        out.writeUTF((String) value);
                        break;
                    default:
                        throw new IOException("Invalid attribute type: " + attr.type);
                }
            }
        }
        return outStream.toByteArray();
    }

    /**
     * Encodes the given page to the end of the page file. Encodes to data to binary starting with the
     * page num, number of records, and then the record data. The array will always be the length of
     * the page size, so any space not taken up by records will contain indeterminate data.
     * @return A byte array containing the fully encoded Page
     */
    public byte[] encodePage() throws IOException {

        System.out.println("Encoding Page: " + pageNumber + " | Total Records: " + numRecords);

        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bs);
        out.writeInt(pageNumber); // Writes the page number first
        out.writeInt(numRecords); // Writes the number of records
        out.write(recordData);    // Writes the record data

        return bs.toByteArray();
    }

}
