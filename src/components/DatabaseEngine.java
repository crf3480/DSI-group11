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
            boolean unique = attributeTokens.contains("unique");
            boolean notNull = attributeTokens.contains("notnull");
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
        try {
            if (!storageManager.deleteTable(tableName)) {
                System.err.println("Table `" + tableName + "` does not exist.");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
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
        Attribute attribute = new Attribute(attributeName, type, false  , false , false ,attributeLength);
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
                    objects[i][j] = records.get(i).get(j);
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
     * Converts a list of strings into their appropriate data objects and inserts the record into a given table.
     * Responsible for checking attribute counts match.
     * @param tableName The table to insert the record into
     * @param tupleValues Strings representing the values for every attribute of the record. Any value which
     *                    was assigned default should be null (null values are just the string "null")
     * @return `true` if the record was inserted. `false` if the insert failed for any reason
     */
    public boolean insert(String tableName, ArrayList<String> tupleValues) {
        TableSchema schema = storageManager.getTableSchema(tableName);
        // Check table exists
        if (schema == null){
            System.err.println("Table " + tableName + " does not exist");
            return false;
        }
        // Check tuple count matches
        if (tupleValues.size() != schema.attributes.size()) {
            System.err.println("Invalid tuple (" + String.join(", ", tupleValues) + "): " +
                    "Attribute count does not match table (Expected: " + schema.attributes.size() + ").");
            return false;
        }
        // Parse and insert record
        try {
            Record record = parseData(schema, tupleValues);
            // Verify that record is unique
            for (Record existingRec : storageManager.getAllInTable(tableName)) {
                int matchAttr = record.duplicate(existingRec, schema);
                if (matchAttr != -1) {
                    System.err.println("Invalid tuple: a record with the value `" + record.get(matchAttr) +
                            "` already exists for column `" + schema.attributes.get(matchAttr).name + "`.");
                    return false;
                }
            }
            storageManager.insertRecord(tableName, record);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Converts a sequence of strings into a Record with a given schema
     * @param schema The schema to match
     * @param row The strings representing the data. A null object indicates that attribute should take its
     *            default value (a null value for the attribute is represented by the String "null")
     * @return The record with the parsed data
     * @throws ClassCastException if an attribute could not be successfully cast its appropriate value
     */
    private Record parseData(TableSchema schema, ArrayList<String> row) throws ClassCastException {
        ArrayList<Object> data = new ArrayList<>();

        for (int i = 0; i < schema.attributes.size(); i++) {
            Attribute attr = schema.attributes.get(i);
            String value = row.get(i);
            // If a String is null, use the default value
            if (value == null) {
                data.add(attr.defaultValue);
                continue;
            }
            // If the string is the *word* "null", check if attribute can be null and set it
            if (value.equals("null")) {
                if (!attr.allowsNull()) {
                    throw new ClassCastException("Invalid value for `" + attr.name +
                            "`: attribute does not allow null values.");
                }
                data.add(null);
                continue;
            }
            switch (attr.type){
                case INT -> {
                    try {
                        if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {    // avoid parseInt from incorrectly accepting "4759"
                            throw new NumberFormatException();
                        }
                        data.add(Integer.parseInt(value));
                    } catch (NumberFormatException e) {
                        throw new ClassCastException("Invalid INT: `" + value + "` not an integer");
                    }
                }
                case CHAR, VARCHAR -> {
                    if (value.charAt(0)!='\"' || value.charAt(value.length()-1)!='\"') {
                        throw new ClassCastException("Invalid " + attr.type + ": `" + value +
                                "` missing encapsulating double quotes");
                    }
                    value = value.substring(1, value.length()-1);       // string has quotes, so it's valid. remove quotes to get the inner string
                    if (value.length() > attr.length){
                        throw new ClassCastException(attr.type + " `" + value +
                                "` exceeds maximum length " + attr.length);
                    }
                    data.add(value);
                }
                case DOUBLE -> {
                    try {
                        if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {    // avoid parseDouble from incorrectly accepting "47.59"
                            throw new ClassCastException("Invalid DOUBLE `" + value + "`: value is a string.");
                        }
                        data.add(Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        throw new ClassCastException("Invalid DOUBLE: " + value + " not a double");
                    }
                }
                case BOOLEAN -> {
                    if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"' ||    // avoid parseBoolean from incorrectly accepting "true"
                            (!value.toLowerCase().equals("true") && !value.toLowerCase().equals("false"))   // booleans can only be true or false
                    ) {
                        throw new ClassCastException("Invalid BOOLEAN: `" + value + "` is not a boolean");
                    }
                    else{
                        data.add(Boolean.parseBoolean(value));
                    }
                }
            }
        }
        return new Record(data);
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
                    String str = (rows[row][col] == null) ? "null" : rows[row][col].toString();
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
        // Add bottom border (if entries exist) and return
        if (rows.length > 0) {
            table.append(horizontalLine);
        }
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