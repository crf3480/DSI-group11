package tableData;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import tableData.AttributeType;

import static tableData.AttributeType.*;

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

    /**
     * Encodes a single record into a byte array output stream, assumes record and attr counts are equal
     * @param ts given table schema to pull attr sizes from
     * @return ByteArrayOutputStream that can be converted to a byte array
     * @throws IOException if there's an issue with a write operation
     */
    public ByteArrayOutputStream encodeRecord(TableSchema ts) throws IOException {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(bs);
        for (int i = 0; i < rowData.size(); i++) {
            AttributeType type = ts.attributes.get(i).type;
            Object value = rowData.get(i);
            switch (type) {
                case INT:
                    ds.writeInt((int)rowData.get(i));
                    break;
                case DOUBLE:
                    ds.writeDouble((double) value);
                    break;
                case BOOLEAN:
                    ds.writeBoolean((boolean) value);
                    break;
                case CHAR:
                case VARCHAR:
                    byte[] stringBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                    ds.write(stringBytes);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported attribute type: " + type);
            }

        }
        ds.flush();
        return bs;
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
