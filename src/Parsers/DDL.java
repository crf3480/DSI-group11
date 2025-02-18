package Parsers;

import java.util.ArrayList;

/**
 * Parser for commands which modify relational schemas
 */
public class DDL {

    public DDL() {

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
