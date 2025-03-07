package tableData;

import java.io.*;
import java.util.*;

public class Catalog {

    private final File catalogFile;
    private int pageSize;
    private int currentTempID;
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
        currentTempID = 0;

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
            AttributeType[] attributeTypes = AttributeType.values();
            // Begin reading tables
            while (true) {
                try {
                    // Read table header data
                    String tableName = inputStream.readUTF();
                    ArrayList<Attribute> attributes = new ArrayList<>();
                    int rootIndex = inputStream.readInt();
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
                    try  {
                        tableSchemas.put(tableName, new TableSchema(tableName, rootIndex, attributes));
                    } catch (IllegalArgumentException e) {
                        System.err.println("Encountered error while creating table from catalog: " + e.getMessage());
                    }
                } catch (EOFException eof) {
                    // Break at end of file
                    break;
                }
            }
            inputStream.close();
        }
    }

    /**
     * Returns a set of all tables present in the catalog
     * @return The set of all table names
     */
    public Set<String> getTableNames() { return tableSchemas.keySet(); }

    /**
     * Returns the next valid name for a temporary table
     * @return The next temp name
     */
    public String nextTempName() {
        currentTempID += 1;
        return String.valueOf(currentTempID);
    }

    /**
     * Inserts a table schema into the catalog
     * @param newSchema The schema to add
     * @return 'true' if the schema was inserted; 'false' if a table with that name already exists in the catalog
     */
    public boolean addTableSchema(TableSchema newSchema){
        if (tableSchemas.containsKey(newSchema.name)) {
            return false;
        }
        tableSchemas.put(newSchema.name, newSchema);
        return true;
    }

    /**
     * Removes a table schema from the catalog
     * @param tableName The name of the table to remove
     * @return 'true' if the table was removed from the schema; 'false' if a table with that name did not exist
     * in the catalog
     */
    public boolean removeTableSchema(String tableName){
        return tableSchemas.remove(tableName) != null;
    }

    /**
     * Fetches the schema of a table by name
     * @param tableName The name of the table
     * @return The table's schema, or 'null' if that table name does not exist in the schema
     */
    public TableSchema getTableSchema(String tableName){
        return tableSchemas.get(tableName);
    }

    /**
     * Updates a table's schema the schema of a table by name
     * @param tableName The name of the table
     * @return The previous schema for that table, if one existed
     */
    public TableSchema setTableSchema(String tableName, TableSchema newSchema){
        return tableSchemas.put(tableName, newSchema);
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
            outputStream.writeInt(tableSchema.rootIndex);
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

    /**
     * Returns a File object pointing to the catalog file
     * @return A File object pointing to the catalog file
     */
    public File getFilePath() {
        return catalogFile;
    }

    /**
     * Returns a File object matching the path to the filename for a table with a given name
     * @param tableName The name of the table
     * @return The table's File object
     */
    public File getTableFile(String tableName){
        return new File(catalogFile.getParent() + File.separator + tableName + ".bin");
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
