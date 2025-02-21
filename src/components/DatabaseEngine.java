package components;
import tableData.TableSchema;

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
        System.out.println("Creating table " + tableName + " with constraints: " + constraints.toString());
        storageManager.createTable(tableName);
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
        // Needs to figure out the primary key of the table and call
        // storage manager function "updateByPrimaryKey" to drop the attribute
    }

    public void addAttribute(String tableName, String attributeName, String attributeType, String defaultValue) {
        // Needs to figure out the primary key of the table and call
        // storage manager function "updateByPrimaryKey" to drop the attribute
        System.out.println("Adding attribute " + attributeName + " to table " + tableName);
        System.out.println(attributeType + " " + defaultValue);
    }


    public void insert(String tableName, ArrayList<String> values) {
        TableSchema schema = storageManager.catalog.getTableSchema(tableName);
        System.out.println(values);
        /*
        if (schema == null){
            System.err.println("Table " + tableName + " does not exist");
        }
        */

        ArrayList<ArrayList<Object>> data = new ArrayList<>();
        ArrayList<Object> currentRow = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i).equals(",")) {
                data.add(parseData(schema, currentRow));
                currentRow = new ArrayList<>();
            }
        }
    }

    private ArrayList<Object> parseData(TableSchema schema, ArrayList<Object> row) {
        ArrayList<Object> data = new ArrayList<>();
        for (int i = 0; i < schema.attributes.size(); i++) {
            switch (schema.attributes.get(i).type){
                case INT -> data.add(Integer.parseInt(row.get(i).toString()));
                case CHAR -> data.add(row.get(i).toString());
                case VARCHAR -> data.add(row.get(i).toString());
                case DOUBLE -> data.add(Double.parseDouble(row.get(i).toString()));
                case BOOLEAN -> data.add(Boolean.parseBoolean(row.get(i).toString()));
            }
        }
        return data;
    }
}
