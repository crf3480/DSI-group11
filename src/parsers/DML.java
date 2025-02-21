package parsers;

import components.DatabaseEngine;
import tableData.Page;
import tableData.Record;
import tableData.TableSchema;
import utils.TestData;

import java.io.IOException;
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
        if (inputList.size() < 7 ||                 // minimum input will have 7 items: insert into <table> values ( <value1> )
            !inputList.get(1).equals("into") ||     // insert requires three keywords (insert (found in Main)
            !inputList.get(3).equals("values") ||   // into, values and both paretheses) to be a valid statement
            !inputList.get(4).equals("(") ||
            !inputList.getLast().equals(")"))
        {
            System.err.println("Invalid insert statement: " + listString(inputList));
            return null;
        }
        else {  // input is also invalid if there aren't commas between multiple records
            for (int i = 0; i < inputList.size(); i++) {
                if (inputList.get(i).equals(")") && (i!=inputList.size()-1 && !inputList.get(i+1).equals(","))) {
                    System.err.println("Invalid insert statement: " + listString(inputList));
                    return null;
                }
            }
        }

        String tableName = inputList.get(2);
        ArrayList<String> values = new ArrayList<>();
        for (String s : inputList.subList(5, inputList.size()-1)) {
            if (!s.equals(")") && !s.equals("(")) {
                values.add(s);
            }
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

    /**
     * Command to allow for simple testing of database
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public String test(ArrayList<String> inputList) {
        ArrayList<Record> records = new ArrayList<>();
        TableSchema ts = TestData.testTableSchema(5);
        System.out.println(ts);
        for (int i = 0; i < 10; i++) {
            records.add(TestData.testRecord(ts));
        }
        Page page = new Page(0, records, 100000, ts);
        System.out.println(page);
        System.out.println("Splitting page...");
        Page splitPage = page.split();
        System.out.println(page);
        System.out.println(splitPage);
        return null;
    }

    private String listString(ArrayList<String> inputList) {
        String output = "";
        for (String s : inputList) {
            output += s+" ";
        }
        return output;
    }
}
