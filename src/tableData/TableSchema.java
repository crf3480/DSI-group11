package tableData;

import java.io.File;
import java.util.ArrayList;

public class TableSchema {

    public String name;
    public String primaryKey = null;
    public int rootIndex; // The location of page 0 in the table, as a number of pageSize chunks
    public ArrayList<Attribute> attributes;
    private String fileDir;

    /**
     * Creates a TableSchema. This should not be directly called by any classes other than Catalog
     * @param name The name of the table
     * @param rootIndex The index of the first page in the table. `-1` if table has no pages
     * @param attributeArrayList An ArrayList containing the table's attributes
     * @param fileDir The directory which contains the table's Page file
     */
    public TableSchema(String name, int rootIndex, ArrayList<Attribute> attributeArrayList, String fileDir) {
        this.name = name;
        this.rootIndex = rootIndex;
        this.attributes = attributeArrayList;
        this.fileDir = fileDir;
        // Verify the attribute names are distinct and there is at least one primary key
        ArrayList<String> attributeNames = new ArrayList<>();
        for (Attribute attribute : attributeArrayList) {
            if (attributeNames.contains(attribute.name)) {
                throw new IllegalArgumentException("Duplicate attribute name: '" + attribute.name + "'");
            }
            attributeNames.add(attribute.name);
            if (attribute.primaryKey) {
                if (primaryKey != null) {
                    throw new IllegalArgumentException("Multiple primary key attributes: '" +
                            attribute.name + "' and '" + primaryKey + "'");
                } else {
                    primaryKey = attribute.name;
                }
            }
        }
        if (primaryKey == null) { throw new IllegalArgumentException("No primary key defined"); }
    }

    /**
     * Returns the index of the attribute with a given name
     * @param attributeName The name of the attribute to look for
     * @return The index of the attribute within the schema. If an attribute with that name does not exist, '-1';
     */
    public int getAttributeIndex(String attributeName) {
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).name.equals(attributeName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns a File object matching the page file for this table
     * @return The table's File object
     */
    public File tableFile(){
        return new File(fileDir + name + ".bin");
    }

    /**
     * Returns the number of attributes in this table that are can be null
     * @return The number of nullable attributes
     */
    public int nullableAttributes() {
        int count = 0;
        for (Attribute attribute : attributes) {
            if (!attribute.notNull && !attribute.primaryKey) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * Creates a shallow duplicate of this TableSchema
     * @return The duplicated TableSchema
     */
    public TableSchema duplicate() {
        ArrayList<Attribute> duplicateAttributes = new ArrayList<>(attributes);
        return new TableSchema(name, rootIndex, duplicateAttributes, fileDir);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Attribute attribute : attributes) {
            sb.append('\t');
            sb.append(attribute.name);
            sb.append(':');
            sb.append(attribute.type);
            if (attribute.type == AttributeType.CHAR || attribute.type == AttributeType.VARCHAR) {
                sb.append('(');
                sb.append(attribute.length);
                sb.append(')');
            }
            if (attribute.primaryKey) { sb.append(" primary key"); }
            if (attribute.unique) { sb.append(" unique"); }
            if (attribute.notNull) { sb.append(" notnull"); }
            sb.append('\n');
        }

        return sb.toString();
    }
}