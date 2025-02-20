package tableData;

import java.util.ArrayList;

public class Record {

    // pass the table class the record is from
    ArrayList<Object> rowData;

    /**
     * Holds the data of a given row
     * @param rowData can be any type
     */
    public Record(ArrayList<Object> rowData) {
        this.rowData = rowData;
    }

    public void encodeRecord(){

    }

    public void decodeRecord(){

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
