package tableData;

import exceptions.CustomExceptions.*;

import java.util.ArrayList;
import java.io.*;
import java.util.Collection;

/**
 * Represents a page of a Table
 * Authors:
 */
public class Page extends Bufferable {

    // The discrepancy between pageSize and recordData size because of additional data
    private final int SIZE_OFFSET = Integer.BYTES; // Record count

    private final TableSchema tableSchema;
    public int pageNumber;

    public ArrayList<Record> records;


    /**
     * Creates a page object from a Page data byte array
     * @param pageIndex The index into the table file where this page is located
     * @param pageNumber The number of the page
     * @param pageData The byte array of page data
     * @param tableSchema The schema of the data in this page
     */
    public Page(int pageIndex, int pageNumber, byte[] pageData, TableSchema tableSchema) throws IOException {
        this.index = pageIndex;
        this.tableSchema = tableSchema;
        if (pageData.length != tableSchema.pageSize) {
            throw new CorruptedDataException("pageData.length did not match DB pageSize when creating page " +
                    pageNumber + " in table `" + tableSchema.name + "` (index: " + pageIndex + ")");
        }

        ByteArrayInputStream inStream = new ByteArrayInputStream(pageData);
        DataInputStream in = new DataInputStream(inStream);
        this.pageNumber = pageNumber;
        int numRecords = in.readInt();
        // Read in records
        byte[] recordData = new byte[pageData.length - SIZE_OFFSET];
        in.readFully(recordData);
        records = decodeRecords(numRecords, recordData);
    }

    /**
     * Creates a Page from a pre-existing list of Records. Page count for TableSchema is automatically updated
     * @param pageIndex The index into the table file where this page is located
     * @param pageNumber The number of the Page
     * @param records The list of records in the Page
     * @param tableSchema The table schema for records in the page
     */
    public Page(int pageIndex, int pageNumber, ArrayList<Record> records, TableSchema tableSchema) {
        this.index = pageIndex;
        this.pageNumber = pageNumber;
        this.tableSchema = tableSchema;
        this.records = records;
        tableSchema.incrementPageCount();
    }

    /**
     * Creates an empty page with a given page number.  Page count for TableSchema is automatically updated
     * @param pageIndex The index into the table file where this page is located
     * @param pageNumber The number of the page
     * @param tableSchema The schema of the records stored in this page
     */
    public Page(int pageIndex, int pageNumber, TableSchema tableSchema) {
        this.index = pageIndex;
        this.pageNumber = pageNumber;
        this.tableSchema = tableSchema;
        this.records = new ArrayList<>();

        // If this is page 0, update the table schema
        if (pageNumber == 0) {
            tableSchema.rootIndex = pageIndex;
        }
        tableSchema.incrementPageCount();
        tableSchema.insertPage(pageNumber, pageIndex);
    }

    /**
     * Gets the number of records contained in the Page
     * @return The number of records stored in this Page
     */
    public int recordCount() {
        return records.size();
    }

    /**
     * Gets the name of the table this page belongs to
     * @return The name of the table
     */
    public String getTableName() {
        return tableSchema.name;
    }

    /**
     * Returns a clone of the Page's TableSchema
     * @return A non-destructive copy of this Page's TableSchema
     */
    public TableSchema getTableSchema() {
        return tableSchema.duplicate();
    }

    /**
     * Returns the number of bytes taken up by all records in this Page
     * @return The number of bytes
     */
    public int pageDataSize() {
        // Calculate the total size of every record in the page
        int totalSize = SIZE_OFFSET;
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
            // CHAR and VARCHAR need their length read directly for each value
            if (attr.type == AttributeType.VARCHAR || attr.type == AttributeType.CHAR) {
                size += 2 * ((String) record.rowData.get(i)).length();
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
     * Page will be given a default page number of -1. 'Half' is determined by data size, not record count.
     * This Page and the child will have their prev/next indices updated, but updating child.next() will still
     * be required.
     * @param childPageIndex The page index that will be assigned to the child page
     * @return The new page containing half the records that were in this Page
     */
    public Page split(int childPageIndex) {
        ArrayList<Record> splitRecords = new ArrayList<>();
        int newSize = 0;
        int pageSize = tableSchema.pageSize;
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
            splitRecords.addFirst(records.removeLast());
            newSize += splitRecordSize;
        }
        Page childPage = new Page(childPageIndex, pageNumber + 1, splitRecords, tableSchema);
        return childPage;
    }

    /**
     * Converts a collection of records into an array of bytes. This array will be at minimum the size of the page,
     * but may be longer if the list of records cannot fit in the page.
     * @param records The records being encoded
     * @return An array of bytes containing the data of the records
     * @throws IOException If there is an error encoding the data
     */
    private byte[] encodeRecords(Collection<Record> records) throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(outStream);
        for (Record record : records) {
            out.write(encodeRecord(record));
        }
        byte[] recordData = new byte[tableSchema.pageSize - SIZE_OFFSET];
        System.arraycopy(outStream.toByteArray(), 0, recordData, 0, outStream.size());
        return recordData;
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
            // For every attribute which can be null, set that bit to '1' if the value is 'null'
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
     * Saves this Page to its corresponding table file
     * @throws IOException if there is an error writing the Page to file
     */
    public void save() throws IOException {
        // Verify table exists
        File tableFile = tableSchema.tableFile();
        if (!tableFile.exists()) {
            throw new IOException("Could not find table file `" + tableFile.getAbsolutePath() + "`");
        }
        // Write data
        try (RandomAccessFile raf = new RandomAccessFile(tableFile, "rw")) {
            long offset = Integer.BYTES + ((long) index * tableSchema.pageSize);  // Page count + pageIndex offset
            raf.seek(offset);

            // Create output byte array
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bs);
            out.writeInt(records.size()); // Writes the number of records
            out.write(encodeRecords(records));    // Writes the record data
            byte[] pageData = bs.toByteArray();
            if (pageData.length > tableSchema.pageSize) {
                System.err.println("Page data array exceeded pageSize while saving");
            }
            // Write output
            raf.write(pageData);
        } catch (IOException ioe) {
            throw new IOException("Encountered problem while attempting to write to table file: " + ioe.getMessage());
        }
    }

    @Override
    public String toString() {
        return tableSchema.name + " - Page #" + pageNumber +
                " (index: " + index + ")" +
                " | Records: " + recordCount();
    }
}
