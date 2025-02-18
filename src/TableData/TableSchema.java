package TableData;

import Parsers.Helpers.Attribute;

import java.util.ArrayList;

public class TableSchema {

    public String name;
    public ArrayList<Attribute> attributeArrayList;

    public TableSchema(String name, ArrayList<Attribute> attributeArrayList) {
        this.name = name;
        this.attributeArrayList = attributeArrayList;
    }
}