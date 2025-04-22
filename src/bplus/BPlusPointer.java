package bplus;

import tableData.Attribute;
import tableData.TableSchema;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class BPlusPointer<T extends Comparable<T>> {

    protected T value;
    protected int pageIndex;
    protected int recordIndex;

    public BPlusPointer(T value, int pageIndex, int recordIndex) {
        this.value = value;
        this.pageIndex = pageIndex;
        this.recordIndex = recordIndex;
    }

    /**
     * Gets the pointer's value
     * @return The value of this pointer
     */
    public T getValue() {
        return value;
    }

    /**
     * Gets the main pointer. For parent nodes, this is
     * @return The value of this pointer
     */
    public int getPageIndex() {
        return pageIndex;
    }

    /**
     * Gets the recordIndex. For parent nodes, this is -1. For leaf nodes, this is the record pointer
     * @return The value of the recordIndex
     */
    public int getRecordIndex() {
        return recordIndex;
    }

    /**
     * Checks if this is a pointer to a record instead of a BPlusNode
     * @return `true` if this is a record pointer; `false` if this object points to a BPlusNode
     */
    public boolean isRecordPointer() {
        return recordIndex != -1;
    }

    /**
     * Compares the values of this RecordPointer with another. Returns a negative
     * integer, zero, or a positive integer if this value is less than, equal to,
     * or greater than the other RecordPointer (respectively).
     * @param o The RecordPointer being compared
     * @return a negative integer, zero, or a positive integer as this RecordPointer
     * is less than, equal to, or greater than the passed RecordPointer
     */
    public int compareTo(BPlusPointer<T> o) {
        return value.compareTo(o.getValue());
    }

    /**
     * Checks if the value in this pointer is greater than another value
     * @param o The value being compared to this RecordPointer
     * @return `true` if the value is less than this pointer's value, or if
     * this pointer has a null value; `false` if the value is greater than
     * or equal to the value in this pointer
     */
    public boolean isGreaterThan(T o) {
        if (value == null) {
            return true;
        }
        return value.compareTo(o) > 0;
    }

    /**
     * Converts the contents of this pointer to a byte array for writing to disk
     * @param schema The TableSchema for table this pointer belongs to
     * @return A byte array representing this pointer's data
     */
    public byte[] encode(TableSchema schema) throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(outStream);
        // Write pointers
        out.writeInt(pageIndex);
        out.writeInt(recordIndex);
        // Write value
        Attribute pk = schema.attributes.get(schema.primaryKey);
        switch (pk.type) {
            case INT -> out.writeInt((Integer)value);
            case DOUBLE -> out.writeDouble((Double)value);
            case BOOLEAN -> out.writeBoolean((Boolean) value);
            case VARCHAR, CHAR -> out.writeUTF((String)value);
        }
        return outStream.toByteArray();
    }

    @Override
    public String toString() {
        return "[" +value +
                ", " + pageIndex +
                ", " + recordIndex +
                ']';
    }
}
