package tableData;

import java.util.ArrayList;

public class TableSchema {

    public String name;
    public ArrayList<Attribute> attributes;

    public TableSchema(String name, ArrayList<Attribute> attributeArrayList) {
        this.name = name;
        this.attributes = attributeArrayList;
    }
}