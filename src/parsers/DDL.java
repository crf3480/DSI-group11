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
        System.out.println("DDL alter");
        return null;
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
