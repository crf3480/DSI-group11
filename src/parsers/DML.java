package parsers;

import components.DatabaseEngine;
import utils.TestData;

import java.util.ArrayList;

/**
 * Parser for commands which modify relational data
 */
public class DML extends GeneralParser {

    DatabaseEngine engine;

    /**
     * Creates a DML parser
     * @param engine A DatabaseEngine for performing the parsed commands
     */
    public DML(DatabaseEngine engine) {
        this.engine = engine;
    }

    /**
     * Performs a display table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String display(ArrayList<String> inputList) {
        engine.displayTable("");
        return null;
    }

    /**
     * Performs a insert table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String insert(ArrayList<String> inputList) {
        if (inputList.size() < 7) {      // minimum input will have 7 items: insert into <table> values ( <value1> )
            //                                    1     2      3      4    5    6     7
            System.err.println("Incomplete insert statement: " + listString(inputList));
            return null;
        }
        else if (!inputList.get(1).equals("into") ||        // insert requires three keywords (insert (found in Main)
                !inputList.get(3).equals("values") ||       // into, values and both paretheses) to be a valid statement
                !inputList.get(4).equals("(") ||
                !inputList.getLast().equals(")")) {
            System.err.println("Invalid insert statement: " + listString(inputList));
            return null;
        }

        String tableName = inputList.get(2);
        ArrayList<String> values = new ArrayList<>();
        for (String s : inputList.subList(5, inputList.size()-1)) {
            values.add(s);
        }

        engine.insert(tableName, values);
        return null;
    }

    /**
     * Performs a select table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String select(ArrayList<String> inputList) {
        return tableToString(TestData.testData(5,10), TestData.testHeaders(5));
    }

    private String listString(ArrayList<String> inputList) {
        String output = "";
        for (String s : inputList) {
            output += s+" ";
        }
        return output;
    }
}
