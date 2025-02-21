package components;

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
    public void createTable(String tableName) {

        storageManager.createTable(tableName);
    }


    /**
     * deletes a table
     * @param tableName The name of the table
     */
    public void deleteTable(String tableName) {
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

    }
}
