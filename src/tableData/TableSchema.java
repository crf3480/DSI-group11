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
     * Returns the number of bytes stored by a record of this schema. If the length is variable (i.e. schema
     * contains a VARCHAR) returns `null`.
     * @return The byte count for a record of this schema. If variable size, returns `null`
     */
    public Integer length() {
        int length = 0;
        for (Attribute attribute : attributes) {
            if (attribute.type == AttributeType.VARCHAR) {
                return null;
            }
            length += attribute.length;
        }
        return length;
    }
}