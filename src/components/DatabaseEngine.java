package components;
import tableData.Attribute;
import tableData.AttributeType;
import tableData.TableSchema;
import utils.TestData;

import java.util.ArrayList;

/**
 * Class for performing SQL actions, as directed by the parsers
 */
public class DatabaseEngine {

    private StorageManager storageManager;

    /**
     * Creates a Database engine
     * @param storageManager A storage manager for retrieving requested data
     */
    public DatabaseEngine(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    /**
     * Creates a table
     * @param tableName The name of the table
     */
    public void createTable(String tableName, ArrayList<String> constraints) {
        //Building out attribute objects using constraints
        System.out.println("Creating table " + constraints.toString());
        ArrayList<Attribute> allAttributes = new ArrayList<>();
        for (String constraint : constraints) {
            String[] parts = constraint.split(" ");
            String attributeName = parts[0];
            String attributeType = parts[1];
            AttributeType type = AttributeType.fromString(parts[1]);
            int attributeLength;
            boolean primaryKey = false;
            boolean unique = false;
            boolean notNull = false;
            String attributeConstraint = "";
            if (attributeType.equals("char") || attributeType.equals("varchar")) {
                 attributeLength = Integer.parseInt(parts[3]);
                 primaryKey = constraint.contains("primarykey");
                 unique = constraint.contains("unique");
                 notNull = constraint.contains("notnull");
                 Attribute currAttribute = new Attribute(attributeName, type, primaryKey, unique, notNull, attributeLength );
                 allAttributes.add(currAttribute);
            } else {
                attributeLength = 0;
                primaryKey = constraint.contains("primarykey");
                unique = constraint.contains("unique");
                notNull = constraint.contains("notnull");
                Attribute currAttribute = new Attribute(attributeName, type, primaryKey, unique, notNull, attributeLength );
                allAttributes.add(currAttribute);
            }
        }
        //All constrains have been created as attributes and added to an arrayList

        storageManager.createTable(tableName, allAttributes);
    }


    /**
     * deletes a table
     * @param tableName The name of the table
     */
    public void deleteTable(String tableName) {
        System.out.println("Deleting table " + tableName);
        storageManager.deleteTable(tableName);
    }



    /**
     * Displays the schema of a table
     * @param tableName The table to display the schema of
     */
    public void displayTable(String tableName) {
        storageManager.displayTable(tableName);
    }

    /**
     * Drops an entire attribute Row from the table
     * @param tableName The table to display the schema of
     */
    //Storage manager
    public void dropAttribute(String tableName, String attributeName) {
        storageManager.deleteAttribute(tableName, attributeName);

    }

    public void addAttribute(String tableName, String attributeName, String attributeType, String defaultValue) {
        String[] parts = attributeType.split(" ");
        AttributeType type = AttributeType.fromString(parts[0]);
        int attributeLength;
        if (parts.length < 3){
            attributeLength = 0;
        } else {
            attributeLength = Integer.parseInt(parts[2]);
        }
        System.out.println(attributeLength);
        Attribute attribute = new Attribute(tableName, type, false  , false , false ,attributeLength);
        storageManager.addAttribute(tableName, attribute);
    }


    public void insert(String tableName, ArrayList<String> values) {
        TableSchema schema = storageManager.getTableSchema(tableName);

        //schema = TestData.permaTable();        //TODO: testing set. delete when complete
        if (schema == null){
            System.err.println("Table " + tableName + " does not exist");
            return;
        }

        ArrayList<ArrayList<Object>> data = new ArrayList<>();
        ArrayList<Object> currentRow = new ArrayList<>();
        for (int i = 0; i <= values.size(); i++) {
            if (i == values.size() || values.get(i).equals(",")) { // every comma denotes a new record, insert record after comma or end of input
                currentRow = parseData(schema, currentRow);
                if(currentRow.size()==schema.attributes.size()){    // row size is only equal to attributes schema if valid
                    //System.out.println("Inserting row " + currentRow.toString());
                    data.add(currentRow);                           // append to new data and reset currentRow
                    currentRow = new ArrayList<>();
                }
                else{
                    break;
                }
            }
            else{
                currentRow.add(values.get(i));  // not a comma, so we keep adding to the current record
            }
        }
        if(data.size()>0){
            //System.out.println(data.size()+" valid record(s) parsed. Sending to Storage Manager for insertion into " + tableName);
            storageManager.insertRecord(tableName, data);
            /*
            for (ArrayList<Object> row : data) {
                System.out.println("\t-\t"+row);
            }
            */
        }
    }

    private ArrayList<Object> parseData(TableSchema schema, ArrayList<Object> row) {
        ArrayList<Object> data = new ArrayList<>();

        //System.out.println(row);
        //System.out.println(schema.toString());
        for (int i = 0; i < schema.attributes.size(); i++) {
            switch (schema.attributes.get(i).type){
                case INT -> {
                    String value = row.get(i).toString();
                    try {
                        if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {    // avoid parseInt from incorrectly accepting "4759"
                            throw new NumberFormatException();
                        }
                        data.add(Integer.parseInt(value));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid INT: "+value+" not an integer");
                        return new ArrayList<>();
                    }
                }
                case CHAR, VARCHAR -> {
                    String value = row.get(i).toString();
                    if (value.charAt(0)!='\"' || value.charAt(value.length()-1)!='\"') {
                        System.err.println("Invalid "+schema.attributes.get(i).type+": "+value+" missing encapsulating double quotes");
                        return new ArrayList<>();
                    }
                    value = value.substring(1, value.length()-1);       // string has quotes, so it's valid. remove quotes to get the inner string
                    if (value.length()>schema.attributes.get(i).length){
                        System.err.println(schema.attributes.get(i).type+" \""+value+"\" exceeds maximum length "+schema.attributes.get(i).length);
                        return new ArrayList<>();
                    }
                    data.add(value);
                }
                case DOUBLE -> {
                    String value = row.get(i).toString();
                    try {
                        if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {    // avoid parseDouble from incorrectly accepting "47.59"
                            throw new NumberFormatException();
                        }
                        data.add(Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid DOUBLE: "+value+" not a double");
                        return new ArrayList<>();
                    }
                }
                case BOOLEAN -> {
                    String value = row.get(i).toString();
                    if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {        // avoid parseBoolean from incorrectly accepting "true"
                        System.err.println("Invalid BOOLEAN: "+value+" is not a boolean");
                        return new ArrayList<>();
                    }
                    data.add(Boolean.parseBoolean(value));
                }
            }
        }
        return data;
    }
}