package tableData;

import java.io.*;
import java.util.ArrayList;

public class Record {

    /// pass the table class the record is from
    ArrayList<Object> rowData;

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
