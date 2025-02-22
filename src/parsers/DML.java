package parsers;
import components.DatabaseEngine;
import tableData.*;
import tableData.Record;
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
    public void display(ArrayList<String> inputList) {
        engine.displayTable("");
    }

    /**
     * Performs a insert table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public void insert(ArrayList<String> inputList) {
        if (inputList.size() < 7 ||                 // minimum input will have 7 items: insert into <table> values ( <value1> )
            !inputList.get(1).equals("into") ||     // insert requires three keywords (insert (found in Main)
            !inputList.get(3).equals("values") ||   // into, values and both paretheses) to be a valid statement
            !inputList.get(4).equals("(") ||
            !inputList.getLast().equals(")"))
        {
            System.err.println("Invalid insert statement: " + String.join(" ", inputList));
            return;
        }
        else {  // input is also invalid if there aren't commas between multiple records
            for (int i = 0; i < inputList.size(); i++) {
                if (inputList.get(i).equals(")") && (i!=inputList.size()-1 && !inputList.get(i+1).equals(","))) {
                    System.err.println("Invalid insert statement: " + String.join(" ", inputList));
                    return;
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
    }

    /**
     * Performs a select table command
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public void select(ArrayList<String> inputList) {
        if (!inputList.getFirst().equals("select") || !inputList.contains("from")) {
            System.err.println("Invalid select statement: " + String.join(" ", inputList));
            return;
        }
        ArrayList<String> columns = new ArrayList<>(inputList.subList(1, inputList.indexOf("from")));
        if (columns.contains("*") && columns.size() > 1) {
            System.err.println("Invalid select statement: " + String.join(" ", inputList));
            return;
        }
        ArrayList<String> tables = new ArrayList<>(inputList.subList(inputList.indexOf("from") + 1, (inputList.contains("where") ? inputList.indexOf("where") : inputList.size())));
        ArrayList<String> where = new ArrayList<>();
        if (inputList.contains("where")) {
            where.addAll(inputList.subList(inputList.indexOf("where")+1, inputList.size()));
        }
        engine.selectRecords(columns, tables, where);
    }

    /**
     * Command to allow for simple testing of database
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. `null` if command produces no output
     */
    public void test(ArrayList<String> inputList) {
        ArrayList<Record> records = new ArrayList<>();
        int pageSize = 2000;
        TableSchema ts = TestData.permaTable();
        Page page = new Page(0, records, pageSize, ts);
        while (page.insertRecord(TestData.testRecord(ts))) { } // Do nothing until the page gets full
        TableSchema newSchema = TestData.permaTable();
        newSchema.attributes.remove(3);
        newSchema.attributes.add(new Attribute("jizz",
                AttributeType.CHAR,
                false,
                true,
                true,
                100,
                "Hallo"));
        System.out.println(page);
        ArrayList<Page> splitPages = page.updateSchema(newSchema);
        System.out.println(page);
        for (Page newPage : splitPages) {
            System.out.println(newPage);
        }
    }
}
