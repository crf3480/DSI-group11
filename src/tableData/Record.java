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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Record: { ");
        for (Object o : rowData) {
            sb.append(o).append(", ");
        }
        sb.append(" }");

        return sb.toString();
    }
}
