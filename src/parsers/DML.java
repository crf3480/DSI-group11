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
        if (inputList.size() < 2) {
            System.err.println("Invalid number of arguments: display (info|schema) <table>;");
            return;
        }
        switch (inputList.get(1)) {
            case "info":
                if (inputList.size() != 3) {
                    System.err.println("Invalid number of arguments: display info <table>;");
                    return;
                }
                engine.displayTable(inputList.get(2));
                break;
            case "schema":
                engine.displaySchema();
                break;
            default:
                System.err.println("Invalid arguments: display (info|schema) <table>;");
        }
    }

    /**
     * Performs an insert table command
     * @param inputList The list of tokens representing the user's input
     */
    public void insert(ArrayList<String> inputList) {
        // Validate input
        if (inputList.size() < 7) {
            System.err.println("Too few arguments for insert: insert into <table> values (<data>);");
            return;
        } else if (!inputList.get(1).equals("into") || !inputList.get(3).equals("values")) {
            System.err.println("Invalid insert command: insert into <table> values (<data>);");
            return;
        } else if (!inputList.get(4).equals("(")) {
            System.err.println("Invalid insert command: records must begin with `(`");
            return;
        } else if (!inputList.getLast().equals(")")) {
            System.err.println("Invalid insert command: unclosed parenthesis.");
            return;
        }

        ArrayList<ArrayList<String>> tuples = new ArrayList<>();
        ArrayList<String> tupleTokens = new ArrayList<>();
        boolean prevWasComma = false;
        for (int i = 4; i < inputList.size(); i++) {
            String s = inputList.get(i);
            if (tupleTokens == null) {  // Null represents a comma has been seen, but no open paren
                if (s.equals(",")) {
                    tupleTokens = new ArrayList<>();
                    continue;
                } else {
                    System.err.println("Invalid insert statement: tuple values must be separated by a comma.");
                    return;
                }
            }
            // Close parenthesis saves the current list and starts a new one
            if (s.equals(")")) {
                if (tupleTokens.isEmpty()) {
                    System.err.println("Invalid insert statement: empty tuple.");
                }
                if (prevWasComma) {
                    tuples.add(null);
                }
                tuples.add(tupleTokens);
                tupleTokens = null;
                continue;
            }
            // Open parenthesis should only be seen if there is not a tuple being actively parsed
            if (s.equals("(")) {
                if (tupleTokens.isEmpty()) {
                    tupleTokens = new ArrayList<>();
                    continue;
                } else {
                    System.err.println("Invalid insert statement: nested parentheses.");
                    return;
                }
            }
            // If the token isn't a comma, add it to the list
            // If it is a comma, add `null` to tuples if the previous value was also a comma, indicating a default value
            if (s.equals(",")) {
                if (prevWasComma) {
                    tupleTokens.add(null);
                }
                prevWasComma = true;
            } else {
                prevWasComma = false;
                tupleTokens.add(s);
            }
        }
        // Insert each record. If an insert fails, cancel
        for (ArrayList<String> tuple : tuples) {
            if (!engine.insert(inputList.get(2), tuple)) {
                return;
            }
        }
    }

    /**
     * Performs a select table command
     * @param inputList The list of tokens representing the user's input
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
