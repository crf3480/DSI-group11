package parsers;
import components.DatabaseEngine;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;

/**
 * Parser for commands which modify relational schemas
 */
public class DDL {

    DatabaseEngine engine;

    /**
     * Creates a DDL parser
     *
     * @param engine A DatabaseEngine for performing the parsed commands
     */
    public DDL(DatabaseEngine engine) {
        this.engine = engine;
    }

    /**
     * Performs an alter table command
     *
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. 'null' if command produces no output
     */
    public String alter(ArrayList<String> inputList) {
        //Check if inital query length is correct
        if (inputList.size() < 5){
            System.err.println("Invalid Alter Statement");
            return null;
        }
        //Splitting the string into qiery
        String queryType = inputList.get(0) + " " + inputList.get(1);
        String tableName = inputList.get(2);
        String addOrDrop = inputList.get(3);
        String attributeName = inputList.get(4);

        //Routes between drop and add
        if (queryType.equals("alter table")) {
            if (addOrDrop.equals("drop")) {
                //Drop an attribute
                engine.dropAttribute(tableName, attributeName);
            } else if (addOrDrop.equals("add")) {
                String attributeType = " ";
                //Add an attribute
                //Need to figure out if attribute type has Parentheses
                int hasParenthesis = inputList.indexOf("(");
                if (hasParenthesis == -1) {
                    attributeType = inputList.get(5);
                } else {
                    attributeType = inputList.get(5) + " ( " + inputList.get(hasParenthesis + 1) + " )";
                }
                //Checking if there is a provided default value
                int defaultIndex = inputList.indexOf("default");
                if (defaultIndex == -1 || inputList.get(defaultIndex).equals("null")) {
                    // Default is null
                    engine.addAttribute(tableName, attributeName, attributeType, null);
                } else {
                   //default value is provided
                    engine.addAttribute(tableName, attributeName, attributeType, inputList.get(defaultIndex + 1));
                }
                return null;
            } else {
                System.err.println("Invalid Alter Statement");
                return null;
            }
        } else {
            System.err.println("Invalid Alter Statement");
            return null;
        }
        return null;

    }

    /**
     * Performs a create table command
     *
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. 'null' if command produces no output
     */
    public String create(ArrayList<String> inputList) {
        // Soft validate input format
        if (inputList.size() < 4){
            System.err.println("Insufficient parameters for 'create table' statement");
            return null;
        }
        if (!inputList.get(1).equals("table") || !inputList.get(3).equals("(") || !inputList.getLast().equals(")")) {
            System.err.println("Invalid 'create table' statement: create table <name>(<attr name> <attr type>...);");
            return null;
        }
        // Read basic table data
        if (Character.isDigit(inputList.get(2).charAt(0))) {    // Table name cannot start with a number
            System.err.println("Invalid table name: "+inputList.get(2));
        }
        String tableName = inputList.get(2);
        ArrayList<ArrayList<String>> attributeList = new ArrayList<>();
        ArrayList<String> attributeTokens = new ArrayList<>();
        for (int i = 4; i < inputList.size() - 1; i++) {
            if (inputList.get(i).equals(",")) {
                attributeList.add(attributeTokens);
                attributeTokens = new ArrayList<>();
            } else {
                attributeTokens.add(inputList.get(i));
            }
        }
        attributeList.add(attributeTokens);
        try {
            engine.createTable(tableName, attributeList);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    /**
     * Performs a drop table command
     *
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. 'null' if command produces no output
     */
    public String drop(ArrayList<String> inputList) {
        //Error handling
        if (inputList.size() < 3){
            System.err.println("Incorrect Create Statement");
            return null;
        }
        //Split string into query
        String queryType = inputList.get(0) + " " + inputList.get(1);
        String tableName = inputList.get(2);
        if (queryType.equals("drop table")) {
            //delete the table
            engine.deleteTable(tableName);
            return null;
        } else {
            System.err.println("Incorrect Drop Statement");
            return null;
        }
    }
}
