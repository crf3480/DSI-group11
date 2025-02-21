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

        schema = TestData.testTableSchema(5);        //TODO: delete when method is complete
        if (schema == null){
            System.err.println("Table " + tableName + " does not exist");
        }

        ArrayList<ArrayList<Object>> data = new ArrayList<>();
        ArrayList<Object> currentRow = new ArrayList<>();
        for (int i = 0; i <= values.size(); i++) {
            if (i == values.size() || values.get(i).equals(",")) { // every comma denotes a new record, insert record after comma or end of input
                currentRow = parseData(schema, currentRow);
                if(currentRow.size()==schema.attributes.size()){
                    data.add(currentRow);
                    currentRow = new ArrayList<>();
                }
                else{
                    System.err.println("Record "+currentRow+" is invalid");
                    break;
                }
            }
            else{
                currentRow.add(values.get(i));
            }

        }

        System.out.println("Inserting "+ data.size() +" records into " + tableName+":");
        for (ArrayList<Object> row : data) {
            System.out.println(row);
        }
    }

    private ArrayList<Object> parseData(TableSchema schema, ArrayList<Object> row) {
        ArrayList<Object> data = new ArrayList<>();
        for (int i = 0; i < schema.attributes.size(); i++) {
            switch (schema.attributes.get(i).type){
                case INT -> data.add(Integer.parseInt(row.get(i).toString()));
                case CHAR, VARCHAR -> {
                    data.add(row.get(i).toString());
                }
                case DOUBLE -> data.add(Double.parseDouble(row.get(i).toString()));
                case BOOLEAN -> data.add(Boolean.parseBoolean(row.get(i).toString()));
            }
        }
        return data;
    }
}
