package components;
import tableData.Attribute;
import tableData.AttributeType;
import tableData.Record;
import tableData.TableSchema;

import java.util.ArrayList;

/**
 * Class for performing SQL actions, as directed by the parsers
 */
public class DatabaseEngine {

    private StorageManager storageManager;

    /**
     * Creates a Database engine
     * @param storageManager A storage manager for retrieving requested data
     */
    public DatabaseEngine(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    /**
     * Creates a table from a sequence of strings
     * @param tableName The name of the table
     * @param attributeList A list of token lists, with each sublist representing all the keywords of an attribute
     * @throws IllegalArgumentException if there is a sequence of strings which does not represent a valid attribute
     */
    public void createTable(String tableName, ArrayList<ArrayList<String>> attributeList) {
        //Building out attribute objects using constraints
        ArrayList<Attribute> allAttributes = new ArrayList<>();
        for (ArrayList<String> attributeTokens : attributeList) {
            String errorMessage = "Invalid attribute declaration: `" + String.join(" ", attributeTokens) + "`. ";
            if (attributeTokens.size() < 2) {
                throw new IllegalArgumentException(errorMessage + "All attributes must have at least a name and type");
            }
            String name = attributeTokens.getFirst();
            boolean primaryKey = attributeTokens.contains("primarykey");
            boolean unique = attributeTokens.contains("unique") || primaryKey;
            boolean notNull = attributeTokens.contains("notnull") || primaryKey;
            int length = 0;
            AttributeType attrType;
            try {
                attrType = AttributeType.fromString(attributeTokens.get(1));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(errorMessage + "Second keyword must be a valid type.");
            }
            if (attrType == AttributeType.CHAR || attrType == AttributeType.VARCHAR) {
                if (attributeTokens.size() < 5 || !attributeTokens.get(2).equals("(") || !attributeTokens.get(4).equals(")")) {
                    throw new IllegalArgumentException(errorMessage + "CHAR and VARCHAR attributes must declare a length.");
                }
                try {
                    length = Integer.parseInt(attributeTokens.get(3));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(errorMessage + "Invalid length parameter `" + attributeTokens.get(2) + "`.");
                }
            }
            allAttributes.add(new Attribute(name, attrType, primaryKey, notNull, unique, length));
        }
        try {
            storageManager.createTable(tableName, allAttributes);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * deletes a table
     * @param tableName The name of the table
     */
    public void deleteTable(String tableName) {
        System.out.println("Deleting table " + tableName);
        storageManager.deleteTable(tableName);
    }

    /**
     * Displays information about a table
     * @param tableName The table to display the details of
     */
    public void displayTable(String tableName) {
        storageManager.displayTable(tableName);
    }

    /**
     * Displays the schema of the database
     */
    public void displaySchema() {
        storageManager.displaySchema();
    }

    /**
     * Drops an entire attribute Row from the table
     * @param tableName The table to display the schema of
     */
    //Storage manager
    public void dropAttribute(String tableName, String attributeName) {
        storageManager.dropAttribute(tableName, attributeName);

    }

    public void addAttribute(String tableName, String attributeName, String attributeType, String defaultValue) {
        String[] parts = attributeType.split(" ");
        AttributeType type = AttributeType.fromString(parts[0]);
        int attributeLength;
        if (parts.length < 3){
            attributeLength = 0;
        } else {
            attributeLength = Integer.parseInt(parts[2]);
        }
        System.out.println(attributeLength);
        Attribute attribute = new Attribute(tableName, type, false  , false , false ,attributeLength);
        storageManager.addAttribute(tableName, attribute);
    }

    /**
     * SELECT name, salary FROM students, teachers WHERE salary = 1000
     *
     * @param columns   Everything between the SELECT and FROM in a select statement
     * @param tables    Everything between the FROM and the WHERE in a select statement
     * @param where     Everything after the WHERE in a select statement
     */
    public void selectRecords(ArrayList<String> columns, ArrayList<String> tables, ArrayList<String> where) {
        //TODO: (IN PHASE 2) this only implements select * from a single table
        try {
            ArrayList<Record> records = storageManager.getAllInTable(tables.get(0));    // select * from table
            if (records == null) { return; }  // If table does not exist, cancel
            TableSchema schema = storageManager.getTableSchema(tables.get(0));
            Object[][] objects = new Object[records.size()][schema.attributes.size()];
            for (int i = 0; i < records.size(); i++) {
                for (int j = 0; j < schema.attributes.size(); j++) {
                    objects[i][j] = records.get(i).rowData.get(j);
                }
            }
            String[] headers = new String[schema.attributes.size()];
            for (int i = 0; i < schema.attributes.size(); i++) {
                headers[i] = schema.attributes.get(i).name;
            }
            System.out.println(tableToString(objects, headers));
        }
        catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

    }

    /**
     * Parses the insert logic for an arbitrary number of records. Input is valid, this checks for data correctness
     * @param tableName The table to insert into
     * @param values    All values between and including the parentheses
     */
    public void insert(String tableName, ArrayList<String> values) {
        TableSchema schema = storageManager.getTableSchema(tableName);

        //schema = TestData.permaTable();        //TODO: testing set. delete when complete
        if (schema == null){
            System.err.println("Table " + tableName + " does not exist");
            return;
        }

        ArrayList<ArrayList<Object>> data = new ArrayList<>();
        ArrayList<Object> currentRow = new ArrayList<>();
        for (int i = 0; i <= values.size(); i++) {
            if (i == values.size() || values.get(i).equals(",")) { // every comma denotes a new record, insert record after comma or end of input
                currentRow = parseData(schema, currentRow);
                if(currentRow.size()==schema.attributes.size()){    // row size is only equal to attributes schema if valid
                    //System.out.println("Inserting row " + currentRow.toString());
                    data.add(currentRow);                           // append to new data and reset currentRow
                    currentRow = new ArrayList<>();
                }
                else{
                    break;
                }
            }
            else{
                currentRow.add(values.get(i));  // not a comma, so we keep adding to the current record
            }
        }
        if(data.size()>0){
            //System.out.println(data.size()+" valid record(s) parsed. Sending to Storage Manager for insertion into " + tableName);
            storageManager.insertRecord(tableName, data);
            /*
            for (ArrayList<Object> row : data) {
                System.out.println("\t-\t"+row);
            }
            */
        }
    }

    private ArrayList<Object> parseData(TableSchema schema, ArrayList<Object> row) {
        ArrayList<Object> data = new ArrayList<>();

        //System.out.println(row);
        //System.out.println(schema.toString());
        for (int i = 0; i < schema.attributes.size(); i++) {
            switch (schema.attributes.get(i).type){
                case INT -> {
                    String value = row.get(i).toString();
                    try {
                        if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {    // avoid parseInt from incorrectly accepting "4759"
                            throw new NumberFormatException();
                        }
                        data.add(Integer.parseInt(value));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid INT: "+value+" not an integer");
                        return new ArrayList<>();
                    }
                }
                case CHAR, VARCHAR -> {
                    String value = row.get(i).toString();
                    if (value.charAt(0)!='\"' || value.charAt(value.length()-1)!='\"') {
                        System.err.println("Invalid "+schema.attributes.get(i).type+": "+value+" missing encapsulating double quotes");
                        return new ArrayList<>();
                    }
                    value = value.substring(1, value.length()-1);       // string has quotes, so it's valid. remove quotes to get the inner string
                    if (value.length()>schema.attributes.get(i).length){
                        System.err.println(schema.attributes.get(i).type+" \""+value+"\" exceeds maximum length "+schema.attributes.get(i).length);
                        return new ArrayList<>();
                    }
                    data.add(value);
                }
                case DOUBLE -> {
                    String value = row.get(i).toString();
                    try {
                        if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {    // avoid parseDouble from incorrectly accepting "47.59"
                            throw new NumberFormatException();
                        }
                        data.add(Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid DOUBLE: "+value+" not a double");
                        return new ArrayList<>();
                    }
                }
                case BOOLEAN -> {
                    String value = row.get(i).toString();
                    if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {        // avoid parseBoolean from incorrectly accepting "true"
                        System.err.println("Invalid BOOLEAN: "+value+" is not a boolean");
                        return new ArrayList<>();
                    }
                    data.add(Boolean.parseBoolean(value));
                }
            }
        }
        return data;
    }

    /**
     * Converts a 2D array into a clean, printed output
     * @param rows The data to display
     * @param headers Optionally, the names of each column. If the length does not match the number of columns in data
     *                blank values will be inserted into the smaller to match. If null, no headers will be displayed
     * @return The string representation of the table
     */
    public String tableToString(Object[][] rows, String[] headers) {
        final String LEFT_WALL = "| ";
        final String RIGHT_WALL = " |\n";
        final String CELL_DIVIDER = " | ";

        // Find max number of columns between headers and rows, with a minimum of 1
        int numCols = Math.max((rows.length == 0) ? 1 : rows[0].length, (headers == null) ? 1 : headers.length);
        String[][] dataStrings = new String[rows.length][numCols];
        int[] colWidths = new int[numCols];  // Max width of data in each column
        // If present, check the header widths
        if (headers != null) {
            for (int i = 0; i < numCols; i++) {
                if (i < headers.length) {
                    colWidths[i] = headers[i].length();
                }
            }
        }
        // Convert data to String, tracking the max width of every String
        // If numCols is greater than the length of the rows, just put an empty String
        for (int col = 0; col < numCols; col++) {
            for (int row = 0; row < rows.length; row++) {
                if (col < rows[row].length) {
                    String str = rows[row][col].toString();
                    dataStrings[row][col] = str;
                    colWidths[col] = Math.max(colWidths[col], str.length());
                } else {
                    dataStrings[row][col] = "";
                }
            }
        }
        // Compute the horizontal dividing line
        StringBuilder horizontalLine = new StringBuilder("+");
        for (int col = 0; col < numCols; col++) {
            horizontalLine.append("-".repeat(colWidths[col] + 1));
            horizontalLine.append("-+");
        }
        horizontalLine.append("\n");

        StringBuilder table = new StringBuilder(horizontalLine);
        // Create header boxes
        if (headers != null) {
            table.append(LEFT_WALL);
            for (int i = 0; i < numCols; i++) {
                if (i < headers.length) {
                    table.append(padded(headers[i], colWidths[i]));
                } else {
                    table.append(" ".repeat(colWidths[i]));
                }
                // Don't print on last col
                if (i < numCols - 1) {
                    table.append(CELL_DIVIDER);
                }
            }
            table.append(RIGHT_WALL);
            table.append(horizontalLine);
        }

        // Pad each cell to length and then print
        for (int row = 0; row < dataStrings.length; row++) {
            table.append(LEFT_WALL);
            for (int col = 0; col < numCols; col++) {
                dataStrings[row][col] = padded(dataStrings[row][col], colWidths[col]);
            }
            table.append(String.join(CELL_DIVIDER, dataStrings[row]));
            table.append(RIGHT_WALL);
        }
        // Add bottom border and return
        table.append(horizontalLine);
        return table.toString();
    }

    /**
     * Pads a string to a given length by appending spaces
     * @param text The text to display
     * @param width The width to pad it to
     * @return The padding string. If string is longer than width, returns string
     */
    private String padded(String text, int width) {
        if (text.length() > width) {
            return text;
        }
        return text + " ".repeat(width - text.length());
    }
}