package tableData;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import tableData.AttributeType;

import static tableData.AttributeType.*;

public class Record {

    // pass the table class the record is from
    ArrayList<Object> rowData;

    /**
     * Holds the data of a given row, assuming you know the values
     * @param rowData can be any type
     */
    public Record(ArrayList<Object> rowData) {
        this.rowData = rowData;
    }

    // Initializer for no row data, use the decode method to populate
    public Record() {
        this.rowData = new ArrayList<>();
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
        for (int i = 0; i < ts.attributes.size(); i++) {
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

    /**
     * Populates an empty record given a set of bytes the size of the record
     * @param ts given table schema
     * @param recordBytes record stored as binary
     * @throws IOException if there is an issue decoding
     */
    public void decodeRecord(TableSchema ts, byte[] recordBytes) throws IOException {
        ByteArrayInputStream bs = new ByteArrayInputStream(recordBytes);
        DataInputStream ds = new DataInputStream(bs);

        for (int i = 0; i < ts.attributes.size(); i++) {
            AttributeType type = ts.attributes.get(i).type;
            Object value;
            switch (type) {
                case INT:
                    value = ds.readInt();
                    break;
                case DOUBLE:
                    value = ds.readDouble();
                    break;
                case BOOLEAN:
                    value = ds.readBoolean();
                    break;
                case CHAR:
                case VARCHAR:
                    StringBuilder sb = new StringBuilder();
                    for(int j = 0; j < ts.attributes.get(i).length; j++){
                        sb.append(ds.readChar());
                    }
                    value = sb.toString();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported attribute type: " + type);
            }
            rowData.add(value);
        }
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
