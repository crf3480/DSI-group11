package tableData;

import java.util.ArrayList;

public class TableSchema {

    public String name;
    public ArrayList<Attribute> attributes;

    public TableSchema(String name, ArrayList<Attribute> attributeArrayList) {
        this.name = name;
        this.attributes = attributeArrayList;
    }

    /**
     * Returns the index of the attribute with a given name
     * @param attributeName The name of the attribute to look for
     * @return The index of the attribute within the schema. If an attribute with that name does not exist, `-1`;
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

    @Override
    public String toString() {
        String out = this.name+": [";
        for (Attribute attribute : attributes) {
            out+=attribute.type;
            if (attribute.type == AttributeType.VARCHAR || attribute.type == AttributeType.CHAR) {
                out+="("+attribute.length+")";
            }
            out+=", ";
        }
        return out.substring(0, out.length()-2)+"]";
    }
}