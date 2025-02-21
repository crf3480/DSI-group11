package tableData;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Catalog {

    private final File catalogFile;
    private int pageSize;
    private HashMap<String, TableSchema> tableSchemas;

    private final byte TYPE_MASK =        0b0000111;
    private final byte PRIMARY_KEY_MASK = 0b1000000;
    private final byte NOT_NULL_MASK =    0b0100000;
    private final byte UNIQUE_MASK =      0b0010000;

    /**
     * On startup grabs file if it's there. If not, creates empty catalog file
     * @param file The file the catalog is stored in. By default, this is catalog.bin
     * @param pageSize The page size for the catalog. If the catalog already exists, the catalog's page size
     *                 will be used instead
     */
    public Catalog(File file, int pageSize) throws IOException {
        this.catalogFile = file;
        this.pageSize = pageSize;
        tableSchemas = new HashMap<>();

        if (!catalogFile.exists()) {  // Create new catalog file
            System.out.println("Creating new catalog file");
            if (!catalogFile.createNewFile()) {
                throw new IOException("Could not create catalog file at " + catalogFile.getAbsolutePath());
            }
            // Write pageSize to file
            DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(catalogFile));
            outputStream.writeInt(pageSize);
            outputStream.close();
        } else {  // Read catalog in from file
            DataInputStream inputStream = new DataInputStream(new FileInputStream(catalogFile));
            // Page size
            this.pageSize = inputStream.readInt();
            System.out.println("Page size: " + this.pageSize);
            AttributeType[] attributeTypes = AttributeType.values();
            // Begin reading tables
            while (true) {
                try {
                    // Read table header data
                    String tableName = inputStream.readUTF();
                    ArrayList<Attribute> attributes = new ArrayList<>();
                    int numAttributes = inputStream.readInt();
                    // Read attributes
                    for (int i = 0; i < numAttributes; i++) {
                        byte flags = inputStream.readByte();
                        boolean primaryKey = (flags & PRIMARY_KEY_MASK) != 0;
                        boolean notNull = (flags & NOT_NULL_MASK) != 0;
                        boolean unique = (flags & UNIQUE_MASK) != 0;
                        int typeNum = flags & TYPE_MASK;
                        AttributeType attrType = attributeTypes[typeNum];
                        int length = 0;
                        if (attrType == AttributeType.CHAR || attrType == AttributeType.VARCHAR) {
                            length = inputStream.readInt();
                        }
                        String typeName = inputStream.readUTF();
                        attributes.add(new Attribute(typeName, attrType, primaryKey, notNull, unique, length));
                    }
                    tableSchemas.put(tableName, new TableSchema(tableName, attributes));
                } catch (EOFException eof) {
                    // Break at end of file
                    break;
                }
            }
            inputStream.close();
        }
    }

    /**
     * Inserts a table schema into the catalog
     * @param newSchema The schema to add
     * @return `true` if the schema was inserted; `false` if a table with that name already exists in the catalog
     */
    public boolean addTableSchema(TableSchema newSchema){
        if (tableSchemas.containsKey(newSchema.name)) { return false; }
        tableSchemas.put(newSchema.name, newSchema);
        return true;
    }

    /**
     * Fetches the schema of a table by name
     * @param tableName The name of the table
     * @return The table's schema
     */
    public TableSchema getTableSchema(String tableName){
        return tableSchemas.get(tableName);
    }

    /**
     * Writes the contents of the catalog to its file on disk
     * @throws IOException If there was an error writing to disk
     */
    public void save() throws IOException {
        DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(catalogFile));
        outputStream.writeInt(pageSize);
        // Write out table data
        List<AttributeType> attributeTypes = Arrays.stream(AttributeType.values()).toList();
        for (TableSchema tableSchema : tableSchemas.values()){
            outputStream.writeUTF(tableSchema.name);
            ArrayList<Attribute> attributes = tableSchema.attributes;
            outputStream.writeInt(attributes.size()); // Number of attributes
            for (Attribute attribute : attributes){
                // Type + constraint flags
                byte flagByte = (byte) attributeTypes.indexOf(attribute.type);
                flagByte += (attribute.primaryKey) ? PRIMARY_KEY_MASK : 0;
                flagByte += (attribute.notNull) ? NOT_NULL_MASK : 0;
                flagByte += (attribute.unique) ? UNIQUE_MASK : 0;
                outputStream.writeByte(flagByte);
                // Length + name
                if (attribute.type == AttributeType.CHAR || attribute.type == AttributeType.VARCHAR) {
                    outputStream.writeInt(attribute.length);
                }
                outputStream.writeUTF(attribute.name);
            }
        }
        outputStream.close();
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Page size: ");
        stringBuilder.append(pageSize);
        stringBuilder.append("\n");
        for (TableSchema tableSchema : tableSchemas.values()) {
            stringBuilder.append("-------------------------\n");
            stringBuilder.append(tableSchema.name);
            stringBuilder.append("\n");
            for (Attribute attribute : tableSchema.attributes) {
                stringBuilder.append(attribute.name);
                stringBuilder.append(": ");
                stringBuilder.append(attribute.type);
                stringBuilder.append("(");
                stringBuilder.append(attribute.length);
                stringBuilder.append(") - ");
                stringBuilder.append((attribute.primaryKey) ? "PRIMARY " : "");
                stringBuilder.append((attribute.notNull) ? "NOT NULL" : "");
                stringBuilder.append((attribute.unique) ? "UNIQUE " : "");
                stringBuilder.append("\n");
            }
        }
        return stringBuilder.toString();
    }
}
