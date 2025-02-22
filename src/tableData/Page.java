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

    // The discrepancy between pageSize and recordData size because of additional data
    private final int SIZE_OFFSET = Integer.BYTES * 2;

    private TableSchema tableSchema;
    private int pageSize;
    public int pageNumber;
    public ArrayList<Record> records;


    /**
     * Creates a page object from a Page data byte array
     * @param pageData The byte array of page data
     * @param tableSchema The schema of the data in this page
     */
    public Page(byte[] pageData, TableSchema tableSchema) throws IOException {
        this.tableSchema = tableSchema;
        pageSize = pageData.length;

        ByteArrayInputStream inStream = new ByteArrayInputStream(pageData);
        DataInputStream in = new DataInputStream(inStream);
        pageNumber = in.readInt();
        int numRecords = in.readInt();
        // Read in records
        byte[] recordData = new byte[pageData.length - SIZE_OFFSET];
        in.readFully(recordData);
        records = decodeRecords(numRecords, recordData);
    }

    /**
     * Creates a Page from a pre-existing list of Records
     * @param pageNumber The number of the Page
     * @param records The list of records in the Page
     * @param pageSize The size of the Page
     * @param tableSchema The table schema for records in the page
     */
    public Page(int pageNumber, ArrayList<Record> records, int pageSize, TableSchema tableSchema) {
        this.pageNumber = pageNumber;
        this.tableSchema = tableSchema;
        this.pageSize = pageSize;
        this.records = records;
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
        this.pageSize = pageSize;
        this.records = new ArrayList<>();
    }

    /**
     * Gets the number of records contained in the Page
     * @return The number of records stored in this Page
     */
    public int recordCount() {
        return records.size();
    }

    /**
     * Returns the number of bytes taken up by all records in this Page
     * @return The number of bytes
     */
    public int pageDataSize() {
        // Calculate the total size of every record in the page
        int totalSize = 0;
        for (Record record : records) {
            totalSize += recordSize(record);
        }
        return totalSize;
    }

    /**
     * Gets the number of bytes taken up by a given record
     * @return The number of bytes
     */
    public int recordSize(Record record) {
        int size = (tableSchema.nullableAttributes() + 7) / 8;  // Bytes to store the null flags
        for (int i = 0; i < tableSchema.attributes.size(); i++) {
            Attribute attr = tableSchema.attributes.get(i);
            // Null values are not recorded and thus take up no space
            if (record.rowData.get(i) == null) { continue; }
            // VARCHARs need their length read directly for each value
            if (attr.type == AttributeType.VARCHAR) {
                size += ((String) record.rowData.get(i)).length();
            } else {
                size += attr.length;
            }
        }
        return size;
    }

    /**
     * Gets the list of records in this Page
     * @return The list of records
     */
    public ArrayList<Record> getRecords() {
        return records;
    }

    /**
     * Inserts a record into the page
     * @param record The record being inserted into the page
     * @return `true` if the record was inserted. `false` if the record could not be inserted because it is full
     */
    public boolean insertRecord(Record record) {
        int recordSize = recordSize(record);
        if (recordSize + pageDataSize() > pageSize) {
            return false;
        }
        records.add(record);
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
     * Converts all records in this page to a new schema
     * @param newSchema The schema to update the records in the Page to match
     * @return If updating the schema results in the Page becoming overfull, this Page will be split until
     * all resulting pages are an appropriate size and all resulting Pages will be returned
     * @throws IllegalArgumentException if the existing records cannot be mapped onto the new schema
     */
    public ArrayList<Page> updateSchema(TableSchema newSchema) {
        Integer[] schemaMap = new Integer[newSchema.attributes.size()];
        for (int i = 0; i < newSchema.attributes.size(); i++) {
            schemaMap[i] = tableSchema.getAttributeIndex(newSchema.attributes.get(i).name);
        }
        System.out.print(" ");
        for (Integer i : schemaMap) {
            System.out.print(i);
            System.out.print(" ");
        }
        System.out.println();
        for (Record record : records) {
            ArrayList<Object> rowData = new ArrayList<>();
            for (int i = 0; i < schemaMap.length; i++) {
                rowData.add(schemaMap[i] == -1 ? newSchema.attributes.get(i).defaultValue : record.rowData.get(schemaMap[i]));
            }
            record.rowData = rowData;
        }
        tableSchema = newSchema;
        ArrayList<Page> newPages = new ArrayList<>();
        while (pageDataSize() > pageSize) {
            newPages.add(split());
        }
        return newPages.isEmpty() ? null : newPages;
    }

    /**
     * Converts a binary array of data into a list of records
     * @param numRecords The number of records contained within the byte array
     * @param recordData The byte array containing the encoded record data
     * @return The list of records that was stored in the data array
     */
    private ArrayList<Record> decodeRecords(int numRecords, byte[] recordData) throws IOException {
        ArrayList<Record> records = new ArrayList<>(numRecords);
        ByteArrayInputStream inStream = new ByteArrayInputStream(recordData);
        DataInputStream in = new DataInputStream(inStream);
        byte[] nullableFlags = new byte[(tableSchema.nullableAttributes() + 7) / 8];
        int nullableFlagBit;
        for (int i = 0; i < numRecords; i++) {
            ArrayList<Object> recordAttr = new ArrayList<>();
            in.readFully(nullableFlags);  // This has no effect if nullable attributes is 0
            nullableFlagBit = 0;
            for (Attribute attr : tableSchema.attributes) {
                // For nullable fields, check if null flag is set
                if (!attr.notNull && !attr.primaryKey) {
                    int nullableMask = 1 << (nullableFlagBit % 8);
                    nullableFlagBit += 1;
                    // If null bit is true, value is null and should be skipped
                    if ((nullableFlags[nullableFlagBit / 8] & nullableMask) != 0) {
                        recordAttr.add(null);
                        continue;
                    }
                }
                // For non-null values, read their values into the row data
                switch (attr.type) {
                    case INT -> recordAttr.add(in.readInt());
                    case DOUBLE -> recordAttr.add(in.readDouble());
                    case BOOLEAN -> recordAttr.add(in.readBoolean());
                    case CHAR, VARCHAR -> recordAttr.add(in.readUTF());
                    default -> throw new IOException("Invalid attribute type: " + attr.type);
                }
            }
            records.add( new Record(recordAttr));
        }
        return records;
    }

    /**
     * Splits the data of this page in half, transferring half to a new Page which is then returned. This new
     * Page will be given a default page number of -1. `Half` is determined by data size, not record count.
     * @return The new page containing half the records that were in this Page
     */
    public Page split() {
        ArrayList<Record> splitRecords = new ArrayList<>();
        int newSize = 0;
        while (newSize < pageSize / 2) {
            // Check if moving the new record over will get the page below half size, ending the split
            int splitRecordSize = recordSize(records.getLast());
            if (newSize + splitRecordSize > pageSize / 2) {
                // If keeping the Record is closer (or equal) to an even split than moving it over, keep it
                if ((pageSize / 2) - newSize <= (newSize + splitRecordSize) - (pageSize / 2)) {
                    break;
                }
            }
            // If not, move it over and update the new size of the current page
            splitRecords.add(records.removeLast());
            newSize += splitRecordSize;
        }

        return new Page(pageNumber + 1, splitRecords, pageSize, tableSchema);
    }

    /**
     * Converts a collection of records into an array of bytes. This array will be at minimum the size of the page,
     * but may be longer if the list of records cannot fit in the page.
     * @param records The records being encoded
     * @return An array of bytes containing the data of the records
     * @throws IOException If there is an error encoding the data
     */
    private byte[] encodeRecords(Collection<Record> records) throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream(pageSize);
        DataOutputStream out = new DataOutputStream(outStream);
        for (Record record : records) {
            out.write(encodeRecord(record));
        }
        return outStream.toByteArray();
    }

    /**
     * Encodes a single record to a binary array
     * @param record The record to encode
     * @return The record's data stored as a byte array
     * @throws IOException If there is an error while encoding the record
     */
    private byte[] encodeRecord(Record record) throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(outStream);
        ArrayList<Attribute> attributes = tableSchema.attributes;
        // Start with the null flags for all nullable attributes
        int nullable = tableSchema.nullableAttributes();
        if (nullable > 0) {
            byte[] nullableBytes = new byte[(nullable + 7) / 8];
            int nullFlagBit = 0;
            // For every attribute which can be null, set that bit to `1` if the value is `null`
            for (int i = 0; i < attributes.size(); i++) {
                if (attributes.get(i).allowsNull()) {
                    if (record.rowData.get(i) == null) {
                        nullableBytes[nullFlagBit / 8] += (byte) (1 << nullFlagBit % 8);
                    }
                    nullFlagBit += 1;
                }
            }
            out.write(nullableBytes);
        }
        // Write out attributes
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            Object value = record.rowData.get(i);
            if (value == null) { continue; }  // Null values are not written to file
            switch (attr.type) {
                case INT -> out.writeInt((Integer) value);
                case DOUBLE -> out.writeDouble((Double) value);
                case BOOLEAN -> out.writeBoolean((Boolean) value);
                case CHAR, VARCHAR -> out.writeUTF((String) value);
                default -> throw new IOException("Invalid attribute type: " + attr.type);
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

        System.out.println("Encoding Page: " + pageNumber + " | Total Records: " + records.size());

        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bs);
        out.writeInt(pageNumber); // Writes the page number first
        out.writeInt(records.size()); // Writes the number of records
        out.write(encodeRecords(records));    // Writes the record data

        return bs.toByteArray();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Page #");
        sb.append(pageNumber);
        sb.append("\n------------\n");
        for (Record record : records) {
            sb.append(record.toString());
            sb.append("\n");
        }
        return sb.toString();
    }
}
