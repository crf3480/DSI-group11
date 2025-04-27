package parsers;
import components.DatabaseEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
     * @return The output of the command. 'null' if command produces no output
     */
    public void display(ArrayList<String> inputList) {
        if (inputList.size() < 2) {
            System.err.println("Invalid number of arguments: display (info|schema) <table>;");
            return;
        }
        switch (inputList.get(1)) {
            case "info" -> {
                if (inputList.size() != 3) {
                    System.err.println("Invalid number of arguments: display info <table>;");
                    return;
                }
                engine.displayTable(inputList.get(2));
            }
            case "schema" -> {
                engine.displaySchema();
            }
            case "tree" ->{
                if(engine.isIndexingEnabled()){
                    engine.displayTree(inputList.get(2));
                }
                else{
                    System.err.println("Indexing is disabled. No B+ trees exist.");
                }
            }
            default ->
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
            System.err.println("Invalid insert command: records must begin with '('");
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
            // If it is a comma, add 'null' to tuples if the previous value was also a comma, indicating a default value
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
        if (!inputList.getFirst().equals("select")) {
            System.err.println("Invalid select statement: " + String.join(" ", inputList));
            return;
        }

        ArrayList<String> currentSet = new ArrayList<>();
        ArrayList<String> attributes = new ArrayList<>();
        ArrayList<String> tables = new ArrayList<>();
        ArrayList<String> where = new ArrayList<>();
        String orderby = null;
        int scanMode = 0;
        for (int x = 1; x < inputList.size(); x++) {
            String s = inputList.get(x);
            if (s.equals("select")) {    //Select is a keyword. if we find it the statement is bad
                System.err.println("Invalid select statement: " + String.join(" ", inputList));
                return;
            }
            switch (scanMode) {
                case 0 -> {     // Adding attributes to select. Searching for "from"
                    if (s.equals("from")) {
                        attributes.addAll(currentSet);
                        currentSet = new ArrayList<>();
                        scanMode = 1;
                    } else if (s.equals("where") || s.equals("orderby")) {
                        System.err.println("Invalid select statement: " + String.join(" ", inputList));
                        return;
                    } else {
                        currentSet.add(s);
                    }
                }
                case 1 -> {     // Adding tables to select from. Searching for "where" or "orderby"
                    if (s.equals("from")) {   // from is now an invalid keyword
                        System.err.println("Invalid select statement: " + String.join(" ", inputList));
                        return;
                    } else if (s.equals("where")) {
                        tables.addAll(currentSet);
                        currentSet = new ArrayList<>();
                        scanMode = 2;
                    } else if (s.equals("orderby")) {
                        tables.addAll(currentSet);
                        currentSet = new ArrayList<>();
                        scanMode = 3;
                    } else {
                        currentSet.add(s);
                    }
                }
                case 2 -> {     // Building WHERE clause. Searching for "orderby" if it exists
                    if (s.equals("from") || s.equals("where")) {  // from and where are now invalid keywords
                        System.err.println("Invalid select statement: " + String.join(" ", inputList));
                        return;
                    } else if (s.equals("orderby")) {
                        scanMode = 3;
                        where.addAll(currentSet);
                        currentSet = new ArrayList<>();
                    } else{
                        currentSet.add(s);
                    }
                }
                case 3 -> {     // ORDERBY clause. Always has a single element.
                    if (x!=inputList.size()-1) {
                        System.err.println("Invalid select statement: " + String.join(" ", inputList));
                        System.err.println("Invalid ORDERBY clause - can only order by one attribute");
                        return;
                    }
                    currentSet.add(s);
                    orderby = inputList.get(x);
                }
            }
        }

        // End of inputList reached. Parse final currentSet value.

        if (scanMode == 0) {    // if the statement has no "from" it's not a valid select statement
            System.err.println("Invalid select statement: " + String.join(" ", inputList));
            System.err.println("Missing FROM clause");
            return;
        }
        else{
            if (currentSet.isEmpty()) {
                System.err.println("Invalid select statement: " + String.join(" ", inputList));
                switch (scanMode){
                    case 1 -> {
                        System.err.println("Incomplete FROM clause");
                    }
                    case 2 -> {
                        System.err.println("Incomplete WHERE clause");
                    }
                    case 3 -> {
                        System.err.println("Incomplete ORDERBY clause");
                    }
                }
                return;
            }
            else{
                if (attributes.size()>1){
                    if (attributes.size()%2!=1 || (commaCount(attributes) != attributes.size()/2)){
                        System.err.println("Invalid select statement: " + String.join(" ", inputList));
                        return;
                    }
                }
                switch (scanMode){  // Will only ever enter this switch in mode 1 or 2
                                    // 0 errors out already if invalid and 3's error checking is done in the loop
                    case 1 -> {
                        if (currentSet.size()>1){
                            if (currentSet.size()%2!=1 || (commaCount(currentSet) != currentSet.size()/2)){
                                System.err.println("Invalid select statement: " + String.join(" ", inputList));
                                return;
                            }
                        }
                        tables.addAll(currentSet);
                    }
                    case 2 -> {
                        if (commaCount(currentSet)!=0){
                            System.err.println("Invalid select statement: " + String.join(" ", inputList));
                            return;
                        }
                        where.addAll(currentSet);
                    }
                }
            }
        }
        /*
        System.out.println("SELECT "+attributes);
        System.out.println("FROM "+tables);
        System.out.println("WHERE "+where);
        System.out.println("ORDER BY "+orderby);
        */
        while (attributes.contains(",")){
            attributes.remove(",");
        }
        while (tables.contains(",")){
            tables.remove(",");
        }
        engine.selectRecords(attributes, tables, where, orderby);
    }


    /**
     * Performs a update record command
     * @param inputList The list of tokens representing the user's input
     */
    public void update(ArrayList<String> inputList) throws IOException {
        String name = inputList.get(1);
        if (!inputList.get(2).equalsIgnoreCase("set")){
            System.err.println("Invalid update statement: Must Contain \"set\" => " + String.join(" ", inputList));
        }
        String column = inputList.get(3);
        if (!inputList.get(4).equalsIgnoreCase("=")){
            System.err.println("Invalid update statement: Must Contain \"column\" => " + String.join(" ", inputList));
        }
        String value = inputList.get(5);
        if (!inputList.get(6).equalsIgnoreCase("where")){
            System.err.println("Invalid update statement: Must Contain \"where\" => " + String.join(" ", inputList));
        }
        String condition = " ";
        for (int x = 7 ; x < inputList.size(); x++) {
            condition += inputList.get(x);
    }
        //System.out.println("Name: " + name + " Column: " + column + " Value: " + value + " Condition: " + condition);
        ArrayList<String> where = new ArrayList<>(inputList.subList(7, inputList.size()));
        engine.updateWhere(name, column, value, where);
    }


    /**
     * Performs a delete record command
     * @param inputList The list of tokens representing the user's input
     */
    public void delete(ArrayList<String> inputList) {
        if (!inputList.getFirst().equals("delete")) {
            System.err.println("Invalid delete statement: " + String.join(" ", inputList));
            return;
        }
        if (!inputList.get(1).equals("from")) {
            System.err.println("Invalid delete statement: delete from <name> where <condition>;");
            return;
        }
        String tablename = inputList.get(2); // Validated in database engine
        if (!inputList.get(3).equals("where")) {
            System.err.println("Invalid delete statement: Invalid where clause");
        }
        ArrayList<String> whereQueries = new ArrayList<>(inputList.subList(4, inputList.size()));


        engine.deleteWhere(tablename, whereQueries);

    }


    private int commaCount(ArrayList<String> set) {
        int count = 0;
        for (String s : set) {
            if (s.equals(",")) {
                count++;
            }
        }
        return count;
    }

    /**
     * Command to allow for simple testing of database
     * @param inputList The list of tokens representing the user's input
     * @return The output of the command. 'null' if command produces no output
     */
    public void test(ArrayList<String> inputList) {
        try {
            engine.test(inputList);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
