package Parsers;

import java.util.ArrayList;

/**
 * Parser for commands which modify relational data
 */
public class DML extends GeneralParser {

    public DML() {

    }

    /**
     * Performs a display table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String display(ArrayList<String> inputList) {
        // Garbage data for testing `tableToString()`
        String[][] bogusData = {
                {"asdf",   "a", "n;apnsr",             "10000000", "asgsg"},
                {"g",      "h", "",                    "gsasgasg", "gsgsg"},
                {"asgh",   "j", "sljsljsljsljslsjlsj", "jjjj",     "sgsgs"},
                {"agawhe", "n", "ajgl",                "asg",      "sgsgs"},
                {"any",    "o", "sg",                  "p",        "sgsgsg"},
        };
        String[] headers = {"pojpjspejg", "A", "100", "baojse", "Sample"};
        return tableToString(bogusData, headers);
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
        return null;
    }
}
