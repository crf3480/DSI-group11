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
     * @return The output of the command. `null` if command produces no output
     */
    public String alter(ArrayList<String> inputList) {
        //Check if inital query is correct
        String queryType = inputList.get(0) + " " + inputList.get(1);
        String tableName = inputList.get(2);
        String addOrDrop = inputList.get(3);
        String attributeName = inputList.get(4);
        System.out.println(inputList.toString());
        if (queryType.equals("alter table")) {
            if (addOrDrop.equals("drop")) {
                //Drop an attribute
                engine.dropAttribute(tableName, attributeName);
            } else if (addOrDrop.equals("add")) {
                String attributeType = " ";
                //Add an attribute
                //Need to figure out if attribute type has Parenthesis
                int hasParenthesis = inputList.indexOf("(");
                if (hasParenthesis == -1) {
                    attributeType = inputList.get(5);
                } else {
                    attributeType = inputList.get(5) + "(" + inputList.get(hasParenthesis + 1) + ")";
                }
                //Checking if there is a provided default value
                int hasDefaultVal = inputList.indexOf("default");
                if (hasDefaultVal == -1) {
                    // No default so setting it to NULL
                    engine.addAttribute(tableName, attributeName, attributeType, "null");
                } else {
                    //Need to account for strings as the default value
                    String defaultVal = inputList.get(hasDefaultVal);
                    engine.addAttribute(tableName, attributeName, attributeType, defaultVal);
                }
                //This should b
                return null;
            } else {
                System.err.println("Incorrect Alter Statement");
                return null;
            }
        } else {
            System.err.println("Incorrect Alter Statement");
            return null;
        }
        return null;

    }

    /**
     * Performs a create table command
     *
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String create(ArrayList<String> inputList) {
        String queryType = inputList.get(0) + " " + inputList.get(1);
        String tableName = inputList.get(2);

        //System.out.println("here" + inputList.toString());
        if (queryType.equals("create table")) {
            int index = (inputList.indexOf("(")) + 1;
            ArrayList<String> constraints = new ArrayList<>();
            String currentQuery = " ";
            //Running from ( to ) and adding constraints
            while (index != inputList.size() - 1){
                String currVal = inputList.get(index);
                if (!(currVal.equals(","))){
                    currentQuery += currVal;
                    System.out.println(currentQuery);
                } else {
                    System.out.println("");
                    constraints.add(currentQuery);
                    currentQuery = " ";
                }
                index++;
            }
            System.out.println("here" + constraints.toString());
            return null;
        } else {
            System.err.println("Incorrect Create Statement");
            return null;
        }
    }

    /**
     * Performs a drop table command
     *
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String drop(ArrayList<String> inputList) {
        String queryType = inputList.get(0) + " " + inputList.get(1);
        String tableName = inputList.get(2);
        if (queryType.equals("drop table")) {
            engine.deleteTable(tableName);
            return null;
        } else {
            System.err.println("Incorrect Drop Statement");
            return null;
        }
    }
}
