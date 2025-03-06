package components;
import tableData.Attribute;
import tableData.AttributeType;
import tableData.Record;
import tableData.TableSchema;

import java.util.*;

/**
 * Class for performing SQL actions, as directed by the parsers
 */
public class DatabaseEngine {

    /// Output constants
    private static final String LEFT_WALL = "| ";
    private static final String RIGHT_WALL = " |";
    private static final String CELL_DIVIDER = " | ";
    private static final char TRUCATION_CHAR = '…';

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
            String errorMessage = "Invalid attribute declaration: '" + String.join(" ", attributeTokens) + "'. ";
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
                    throw new IllegalArgumentException(errorMessage + "Invalid length parameter '" + attributeTokens.get(2) + "'.");
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
                System.err.println("Table '" + tableName + "' does not exist.");
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
        AttributeType type;
        try{
            type = AttributeType.fromString(parts[0]);
        }catch (IllegalArgumentException e) {
            System.err.println("Invalid attribute type: " + attributeType);
            return;
        }

        int attributeLength;
        if (parts.length < 3){
            attributeLength = 0;
        } else {
            attributeLength = Integer.parseInt(parts[2]);
        }
        Attribute attribute = new Attribute(attributeName, type, false  , false , false ,attributeLength);
        storageManager.addAttribute(tableName, attribute);
    }

    /**
     * @param attributes    Names of all attributes to be selected from the tables. Comma separated.
     * @param tables        Names of all tables that the attributes will be selected from. Comma separated.
     * @param where         The entirety of the where clause.
     * @param orderby       Attribute to order by in ascending order.
     */
    public void selectRecords(ArrayList<String> attributes, ArrayList<String> tables, ArrayList<String> whereClause, String orderby) {
        //TODO: (IN PHASE 2) this only implements select * from a single table
        try {
            ArrayList<Record> records = storageManager.getAllInTable(tables.get(0));    // select * from table
            if (records == null) { return; }  // If table does not exist, cancel
            TableSchema schema = storageManager.getTableSchema(tables.get(0));
            System.out.println(headerToString(schema, 10));
            System.out.println(tableToString(records, 10));
            System.out.println(footerString(schema, 10));
        }
        catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

    }

    /**
     *
     * @param where
     * @return
     */
    private ArrayList<String> parseWhere(ArrayList<String> where) {
        ArrayList<String> out = new ArrayList<>();
        Stack<String> operators = new Stack<>();
        Stack<String> operands = new Stack<>();

        for (String s : where) {

        }


        return where;
    }

    /**
     * Converts a list of strings into their appropriate data objects and inserts the record into a given table.
     * Responsible for checking attribute counts match.
     * @param tableName The table to insert the record into
     * @param tupleValues Strings representing the values for every attribute of the record. Any value which
     *                    was assigned default should be null (null values are just the string "null")
     * @return 'true' if the record was inserted. 'false' if the insert failed for any reason
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
                    System.err.println("Invalid tuple: a record with the value '" + record.get(matchAttr) +
                            "' already exists for column '" + schema.attributes.get(matchAttr).name + "'.");
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
                    throw new ClassCastException("Invalid value for '" + attr.name +
                            "': attribute does not allow null values.");
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
                        throw new ClassCastException("Invalid INT: '" + value + "' not an integer");
                    }
                }
                case CHAR, VARCHAR -> {
                    if (value.charAt(0)!='\"' || value.charAt(value.length()-1)!='\"') {
                        throw new ClassCastException("Invalid " + attr.type + ": '" + value +
                                "' missing encapsulating double quotes");
                    }
                    value = value.substring(1, value.length()-1);       // string has quotes, so it's valid. remove quotes to get the inner string
                    if (value.length() > attr.length){
                        throw new ClassCastException(attr.type + " '" + value +
                                "' exceeds maximum length " + attr.length);
                    }
                    data.add(value);
                }
                case DOUBLE -> {
                    try {
                        if (value.charAt(0)=='\"' || value.charAt(value.length()-1)=='\"') {    // avoid parseDouble from incorrectly accepting "47.59"
                            throw new ClassCastException("Invalid DOUBLE '" + value + "': value is a string.");
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
                        throw new ClassCastException("Invalid BOOLEAN: '" + value + "' is not a boolean");
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
     * Returns a string containing the properly formatted header to a table printout
     * @param schema The schema of the table being printed
     * @param colWidth The width of every column in the printout. Any attribute name which
     *                 doesn't fit will be truncated
     * @return The header printout
     */
    public String headerToString(TableSchema schema, int colWidth) {
        int totalWidth = LEFT_WALL.length() + RIGHT_WALL.length();
        totalWidth += (colWidth + CELL_DIVIDER.length()) * (schema.attributes.size());
        totalWidth -= CELL_DIVIDER.length(); // Last cell doesn't have divider
        // Add top line
        StringBuilder output = new StringBuilder("+");
        output.append("-".repeat(totalWidth - 2));
        output.append("+\n");
        output.append(LEFT_WALL);

        // Print each header cell
        for (int i = 0; i < schema.attributes.size(); i++) {
            if (i != 0) {
                output.append(CELL_DIVIDER);
            }
            output.append(fitToWidth(schema.attributes.get(i).name, colWidth));
        }
        output.append(RIGHT_WALL);
        output.append('\n');
        // Add bottom line and return
        output.append('+');
        output.append("-".repeat(totalWidth - 2));
        output.append('+');
        return output.toString();
    }

    /**
     * Returns the
     * @param schema The schema of the table being printed
     * @param colWidth The width of every column in the printout. Any attribute name which
     *                 doesn't fit will be truncated
     * @return The header printout
     */
    public String footerString(TableSchema schema, int colWidth) {
        int totalWidth = LEFT_WALL.length() + RIGHT_WALL.length();
        totalWidth += (colWidth + CELL_DIVIDER.length()) * (schema.attributes.size());
        totalWidth -= CELL_DIVIDER.length(); // Last cell doesn't have divider
        return '+' + "-".repeat(totalWidth - 2) + '+';
    }

    /**
     * Converts a list of records to a printable output
     * @param records The data to display
     * @param colWidth The width to use for each column. Any cell value longer than this amount will be truncated
     * @return The string representation of the table
     */
    public String tableToString(Collection<Record> records, int colWidth) {
        StringBuilder output = new StringBuilder();

        // Pad each cell to length and then print
        for (Record record : records) {
            output.append(LEFT_WALL);
            for (int i = 0; i < record.rowData.size(); i++) {
                if (i != 0) {
                    output.append(CELL_DIVIDER);
                }
                if (record.rowData.get(i) == null) {
                    output.append(fitToWidth("null", colWidth));
                } else {
                    output.append(fitToWidth(record.rowData.get(i).toString(), colWidth));
                }
            }
            output.append(RIGHT_WALL);
            output.append('\n');
        }
        // Remove last new line
        output.deleteCharAt(output.length() - 1);
        return output.toString();
    }

    /**
     * Truncates/pads a given String to a given length
     * @param text The String to fit
     * @param width The length to set it to.
     * @return The updated String
     */
    private String fitToWidth(String text, int width) {
        if (text.length() == width) {
            return text;
        } else if (text.length() > width) {
            return text.substring(0, width - 1) + TRUCATION_CHAR;
        }
        return text + " ".repeat(width - text.length());
    }
}