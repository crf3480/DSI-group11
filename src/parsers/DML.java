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
}
