package parsers;

import components.DatabaseEngine;

import java.util.ArrayList;

/**
 * Parser for commands which modify relational schemas
 */
public class DDL {

    DatabaseEngine engine;

    /**
     * Creates a DDL parser
     * @param engine A DatabaseEngine for performing the parsed commands
     */
    public DDL(DatabaseEngine engine) {
        this.engine = engine;
    }

    /**
     * Performs an alter table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String alter(ArrayList<String> inputList) {
        //Check if inital query is correct
        String tableName = inputList.get(0);
        if (inputList.get(0).equals("alter") && inputList.get(1).equals("table")){
            if (inputList.get(4).equals("drop")){

            } else if (inputList.get(4).equals("add")){}

            engine.createTable(tableName); //This should b
            return null;
        } else {
            System.err.println("Incorrect Alter Statement");
            return null;
        }


    }

    /**
     * Performs a create table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String create(ArrayList<String> inputList) {
        engine.createTable("");
        return null;
    }

    /**
     * Performs a drop table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String drop(ArrayList<String> inputList) {
        return null;
    }
}
