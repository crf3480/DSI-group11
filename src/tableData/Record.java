package tableData;

import java.util.ArrayList;

public class Record {

    /// pass the table class the record is from
    public ArrayList<Object> rowData;

    /**
     * Holds the data of a given row, assuming you know the values
     * @param rowData can be any type
     */
    public Record(ArrayList<Object> rowData) {
        this.rowData = rowData;
    }

    /**
     * Initializer for no row data, use the decode method to populate
     */
    public Record() {
        this.rowData = new ArrayList<>();
    }

    /**
     * Returns the number of attributes in the record
     * @return The number of attributes
     */
    public int size() {
        return rowData.size();
    }

    /**
     * Returns the object stored in this record at a given row
     * @param index The index of the attribute to fetch
     * @return The object stored at that index
     */
    public Object get(int index) {
        return rowData.get(index);
    }

    /**
     * Updates the value of an attribute at a given index
     * @param index The index of the attribute to replace
     * @param value The value to replace the specified index with
     * @return The value previously stored at that index
     */
    public Object update(int index, Object value) {
        return rowData.set(index, value);
    }

    /**
     * Checks if this record and another are equivalent under a given schema, comparing any fields marked 'unique'
     * @param other The other record to compare
     * @param schema The schema to compare the records by
     * @return If the records are considered a duplicate under this schema, returns the index of the attribute
     * they match on; Otherwise, -1
     */
    public int isEquivalent(Record other, TableSchema schema) {
        if (rowData.size() != other.size()) { return -1; }  // Records do not match
        for (int i = 0; i < rowData.size(); i++) {
            if ((schema.attributes.get(i).unique || schema.attributes.get(i).primaryKey)
                    && (rowData.get(i) == other.rowData.get(i) ||
                    (rowData.get(i) != null && rowData.get(i).equals(other.rowData.get(i))))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns a shallow copy of this record
     * @return A shallow copy of this Record
     */
    public Record duplicate() {
        ArrayList<Object> duplicateValues = new ArrayList<>(rowData);
        return new Record(duplicateValues);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Record: { ");
        for (Object o : rowData) {
            sb.append(o).append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(" }");

        return sb.toString();
    }

    /**
     * Determines whether this record should come before or after another when sorted by a specified attribute
     * @param other The other record to compare to
     * @param schema The schema of the records
     * @param attrIndex The index of the attribute to order by
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
     * or greater than the other Record.
     */
    public int compareByAttribute(Record other, TableSchema schema, int attrIndex) {
        return switch (schema.attributes.get(schema.primaryKey).type) {
            case INT -> ((Integer) rowData.get(attrIndex)).compareTo(((Integer) other.rowData.get(attrIndex)));
            case DOUBLE -> ((Double) rowData.get(attrIndex)).compareTo(((Double) other.rowData.get(attrIndex)));
            case CHAR, VARCHAR ->
                    ((String) rowData.get(attrIndex)).compareTo(((String) other.rowData.get(attrIndex)));
            case BOOLEAN ->
                    ((Boolean) rowData.get(attrIndex)).compareTo(((Boolean) other.rowData.get(attrIndex)));
        };
    }
}
