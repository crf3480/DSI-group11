package components;
import exceptions.InvalidAttributeException;
import tableData.*;
import tableData.Record;

import where.Evaluator;

import java.io.IOException;
import java.util.*;

/**
 * Class for performing SQL actions, as directed by the parsers
 */
public class DatabaseEngine {

    private final StorageManager storageManager;

    /**
     * Creates a Database engine
     * @param storageManager A storage manager for retrieving requested data
     */
    public DatabaseEngine(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    // ====================================================================================
    //region Commands =====================================================================
    // ====================================================================================

    /**
     * Creates a table from a sequence of strings
     * @param tableName The name of the table
     * @param attributeList A list of token lists, with each sublist representing all the keywords of an attribute
     * @throws IllegalArgumentException if there is a sequence of strings which does not represent a valid attribute
     */
    public void createTable(String tableName, ArrayList<ArrayList<String>> attributeList) {
        //Building out attribute objects using constraints
        ArrayList<Attribute> allAttributes = new ArrayList<>();
        boolean hasPrimaryKey = false;
        for (ArrayList<String> attributeTokens : attributeList) {
            String errorMessage = "Invalid attribute declaration: '" + String.join(" ", attributeTokens) + "'. ";
            if (attributeTokens.size() < 2) {
                throw new IllegalArgumentException(errorMessage + "All attributes must have at least a name and type");
            }
            String name = attributeTokens.getFirst();
            boolean primaryKey = attributeTokens.contains("primarykey");
            if (primaryKey) {
                if (hasPrimaryKey) {
                    System.err.println("ERROR: Cannot create a table with multiple primary keys.");
                    return;
                }
                hasPrimaryKey = true;
            }
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
        if (!hasPrimaryKey) {
            System.err.println("ERROR: Table has no primary key.");
            return;
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

    public void updateWhere(String tableName, String columnName, String newValue, ArrayList<String> condition ) throws IOException {
        TableSchema schema = storageManager.getTableSchema(tableName);
        if (schema == null) {
            System.err.println("Table '" + tableName + "' does not exist.");
            return;
        }
        int attributeIndex = schema.getAttributeIndex(columnName);
        if (attributeIndex == -1) {
            System.err.println("Table '" + tableName + "' does not contain column '" + columnName + "'.");
            return;
        }
        Attribute attribute = schema.attributes.get(attributeIndex);
        Evaluator eval = new Evaluator(condition, schema);

        for(int x = 0; x < schema.pageCount(); x++) {
            Page currPage = storageManager.getPage(schema, x);
            for (Record record : currPage.getRecords()) {
                if (eval.evaluateRecord(record)) {
                    // Delete record from the arraylist
                    currPage.getRecords().remove(record);
                    // Decrement record count
                    schema.decrementRecordCount();
                    // If only record in page delete the page
                    if (schema.recordCount() == 0){
                        storageManager.deletePage(schema, currPage.pageNumber);
                    }
                    // reinsert record into the table
                    updateAndTypeCast(newValue, attributeIndex, attribute, record);
                    if (record != null){
                        storageManager.insertRecord(schema, record);
                    } else {
                        System.err.println("Record '" + record + "' does not exist, error while inserting");
                    }
                }
            }
        }
    }

    /**
     * typecasts an the attribute and calls the update method
     * @param newValue string value to be casted
     * @param attributeIndex index in the attribute (column)
     * @param attribute used for getting the type
     * @param record given record to be updated
     */
    private Record updateAndTypeCast(String newValue, int attributeIndex, Attribute attribute, Record record) {
        switch (attribute.type){
            case INT:
                record.update(attributeIndex, Integer.parseInt(newValue));
                break;
            case DOUBLE:
                record.update(attributeIndex, Double.parseDouble(newValue));
                break;
            case BOOLEAN:
                record.update(attributeIndex, Boolean.parseBoolean(newValue));
                break;
            case CHAR:
            case VARCHAR:
                record.update(attributeIndex, newValue);
            default:
                System.err.println("ERROR: Unsupported attribute type '" + attribute.type + "'.");
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
     * Drops an attribute from a given table
     * @param tableName The name of the table the attribute is dropped from
     * @param attributeName The name of the attribute being dropped
     */
    //Storage manager
    public void dropAttribute(String tableName, String attributeName) {
        TableSchema schema = storageManager.getTableSchema(tableName);
        if (schema == null) {
            System.err.println("Table `" + tableName + "` does not exist.");
            return;
        }
        int dropIndex = schema.getAttributeIndex(attributeName);
        // Validate parameters
        if (dropIndex == -1) {
            System.err.println("No attribute `" + attributeName + "` on table `" + schema.name + "`");
            return;
        }
        if (schema.getAttributeIndex(attributeName) == schema.primaryKey) {
            System.err.println("Cannot drop primary key `" + attributeName + "` from table `" + schema.name + "`");
        }
        // Create a temporary table to transfer the updated info into
        TableSchema newSchema;
        try {
            String tempName = storageManager.getTempTableName();
            ArrayList<Attribute> newAttrList = new ArrayList<>(schema.attributes);
            newAttrList.remove(dropIndex);
            newSchema = storageManager.createTable(tempName, newAttrList);
        } catch (IOException ioe) {
            System.err.println("Encountered error while cloning table: " + ioe + " : " + ioe.getMessage());
            return;
        }
        // Iterate over all records, dropping the attribute and inserting it into the new table
        Page currPage = storageManager.getPage(schema, 0);
        int currIndex = 0;
        while (currPage != null) {
            for (Record r : currPage.records) {
                Record updatedRec = r.duplicate();
                updatedRec.rowData.remove(dropIndex);
                storageManager.fastInsert(newSchema, updatedRec);
            }
            currIndex += 1;
            currPage = storageManager.getPage(schema, currIndex);
        }
        storageManager.replaceTable(schema, newSchema);
    }

    /**
     * Adds an attribute to a specified table
     * @param tableName The name of the table to add the attribute to
     * @param attributeName The name of the attribute being added
     * @param attributeType The type of the new attribute
     * @param defaultValue The default value of the new attribute
     */
    public void addAttribute(String tableName, String attributeName, String attributeType, String defaultValue) {
        TableSchema schema = storageManager.getTableSchema(tableName);
        // Validate parameters
        if (schema == null) {
            System.err.println("Table `" + tableName + "` does not exist.");
            return;
        } else if (schema.getAttributeIndex(attributeName) != -1) {
            System.err.println("Attribute `" + attributeName + "` already exists on table `" + tableName + "`.");
            return;
        }
        String[] parts = attributeType.split(" ");
        AttributeType attrType;
        try{
            attrType = AttributeType.fromString(parts[0]);
        }catch (IllegalArgumentException e) {
            System.err.println("Invalid attribute type: " + attributeType);
            return;
        }
        // Parse attribute length in case of CHAR or VARCHAR
        int attributeLength = 10;
        if (parts.length >= 3){
            try {
                attributeLength = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                System.err.println("Cannot parse length parameter `" + parts[2] + "`.");
                return;
            }
            // Make sure the number makes sense
            if (attrType != AttributeType.CHAR && attrType != AttributeType.VARCHAR) {
                System.err.println("Cannot define custom length for non-character type " + attrType.toString());
                return;
            } else if (attributeLength < 1) {
                System.err.println("Invalid attribute length `" + attributeLength +
                        "`. Length must be a positive integer");
                return;
            } else if (attributeLength > storageManager.pageSize()) {
                System.err.println("Attribute length `" + attributeLength + "` exceeds page size (" +
                        storageManager.pageSize() + ").");
                return;
            }
        }
        Object defaultObj = null;
        if (defaultValue != null) {
            try {
                defaultObj = attrType.parseString(defaultValue);
            } catch (IllegalArgumentException e) {
                System.err.println(e.getMessage());
                return;
            }
        }

        Attribute newAttribute = new Attribute(attributeName,
                attrType,
                false,
                false,
                false,
                attributeLength,
                defaultObj);
        // Create a temporary table to transfer the updated info into
        TableSchema newSchema;
        try {
            String tempTable = storageManager.getTempTableName();
            ArrayList<Attribute> newAttrList = new ArrayList<>(schema.attributes);
            newAttrList.add(newAttribute);
            newSchema = storageManager.createTable(tempTable, newAttrList);
        } catch (IOException ioe) {
            System.err.println("Encountered error while cloning table: " + ioe + " : " + ioe.getMessage());
            return;
        }
        // Iterate over all records, adding the attribute and inserting them into the new table
        Page currPage = storageManager.getPage(schema, 0);
        int currPageNum = 0;
        while (currPage != null) {
            for (Record r : currPage.records) {
                Record updatedRec = r.duplicate();
                updatedRec.rowData.add(defaultObj);
                storageManager.fastInsert(newSchema, updatedRec);
            }
            currPageNum += 1;
            currPage = storageManager.getPage(schema, currPageNum);
        }
        // Once the all records have been updated, swap the temp table with the real one
        storageManager.replaceTable(schema, newSchema);
    }

    /**
     * @param attributes    Names of all attributes to be selected from the tables
     * @param tables        Names of all tables that the attributes will be selected from
     * @param whereClause   The entirety of the where clause.
     * @param orderby       Attribute to order by in ascending order.
     */
    public void selectRecords(ArrayList<String> attributes, ArrayList<String> tables, ArrayList<String> whereClause, String orderby) {
        boolean dropSelectedTable = false;
        for (String table: tables) {    // make sure all given tables exist
            if (storageManager.getTableSchema(table) == null) {
                System.err.println("Invalid select: Table " + table + " does not exist.");
                return;
            }
        }
        if (!allUnique(tables)){
            System.err.println("Invalid select: Cannot join a table with itself.");
            return;
        }
        if (attributes.contains("*") && attributes.size() > 1) {    // * can only be used if it's the only one
            System.err.println("Invalid select: '*' cannot be used while also specifying attributes.");
            return;
        }


        // Join all tables together
        TableSchema schema = storageManager.getTableSchema(tables.get(0));
        for (String table: tables.subList(1, tables.size())) {
            schema = cartesianJoin(schema, storageManager.getTableSchema(table));
        }

        if(!attributes.contains("*") || tables.size() > 1) {
            dropSelectedTable = true;
            schema = projection(schema, attributes);
            if (schema == null) {
                return;
            }
        }

        /*
            we only need to look at the record data if there's a where clause or an orderby attribute
            if so, we make another temp table and run the necessary checks on all records
        */
        if (!whereClause.isEmpty() || !orderby.isEmpty()){
            Evaluator eval = new Evaluator(whereClause, schema);
            int pageIndex = 0;
            Page page = storageManager.getPage(schema, 0);

            // Create temp table
            TableSchema temp;
            try{
                temp = storageManager.createTable(storageManager.getTempTableName(), schema.attributes);
            } catch (IOException e) {
                System.err.println("Encountered error while creating temp table: " + e);
                return;
            }
            // Fill the temp table only with where-passing values
            while (page != null) {
                for (Record r : page.records) {
                    if (eval.evaluateRecord(r)) {   //this will always return true if there's n where clause
                        if (orderby.isEmpty()){     //if we don't care about order, just throw everything that passes in
                            storageManager.fastInsert(temp, r);
                        }
                        else{
                            //TODO: fuckfuckfuckfuckfuckfuckfuckfuck
                        }

                    }
                }
                pageIndex++;
                page = storageManager.getPage(schema, pageIndex);
            }
            storageManager.replaceTable(schema, temp);
        }

        // Print temp table
        System.out.println(headerToString(schema, 10));
        try {
            int pageIndex = 0;
            Page currPage = storageManager.getPage(schema, pageIndex);
            while (currPage != null) {
                System.out.println(tableToString(currPage.records, 10));
                pageIndex += 1;
                currPage = storageManager.getPage(schema, pageIndex);
            }
        }
        catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
        // Cap off with footer string
        System.out.println(footerString(schema, 10));
        if (dropSelectedTable) {
            try{
                storageManager.deleteTable(schema.name);
            } catch (IOException e) {
                System.err.println("Encountered error while deleting table: " + e);
            }
        }
    }

    /**
     * fs records where clause is true
     * @param tableName Given table name to delete record from
     * @param whereClause Arraylist of strings in the where
     */
    public void deleteWhere(String tableName, ArrayList<String> whereClause) {
        TableSchema schema = storageManager.getTableSchema(tableName);
        if (schema == null) {
            System.err.println("Table `" + tableName + "` does not exist.");
            return;
        }

        Evaluator eval = new Evaluator(whereClause, schema);
        int pageIndex = 0;
        Page page = storageManager.getPage(schema, 0);

        // Create temp table
        TableSchema updatedSchema = null;
        String tempName = storageManager.getTempTableName();
        try{
            updatedSchema = storageManager.createTable(
                    tempName,
                    schema.attributes);
        } catch (IOException e) {
            System.err.println("Encountered error while creating temp table: " + e);
            return;
        }

        // Iter through each record and insert into temp table if condition not met
        while (page != null) {
            for (Record r : page.records) {
                if (!eval.evaluateRecord(r)) {
                    storageManager.fastInsert(updatedSchema, r);
                }
            }
            pageIndex++;
            page = storageManager.getPage(schema, pageIndex);
        }
        // Replace the old record list with the new record list
        storageManager.replaceTable(schema, updatedSchema);

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
            int pageIndex = 0;
            Page currPage = schema.rootIndex == -1 ? null : storageManager.getPage(schema, pageIndex);
            // Verify record is unique
            while (currPage != null) {
                for (Record r : currPage.records) {
                    int matchAttr = record.isEquivalent(r, schema);
                    if (matchAttr != -1) {
                        System.err.println("Invalid tuple: a record with the value '" + record.get(matchAttr) +
                                "' already exists for column '" + schema.attributes.get(matchAttr).name + "'.");
                        return false;
                    }
                }
                pageIndex += 1;
                currPage = storageManager.getPage(schema, pageIndex);
            }
            // Insert
            storageManager.insertRecord(schema, record);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }

    //endregion

    // ====================================================================================
    //region Joins/Projections ============================================================
    // ====================================================================================

    /**
     * Takes two TableSchemas and attributes so that any shared names are changed to
     * "tableName.attributeName"
     * @param schema1 The first schema
     * @param schema2 The second schema
     */
    private void dedupeAttrNames(TableSchema schema1, TableSchema schema2) {
        // Create the set of shared attr names
        HashSet<String> schemaNames = new HashSet<>();
        for (Attribute attr : schema1.attributes) {
            schemaNames.add(attr.name);
        }
        HashSet<String> otherNames = new HashSet<>();
        for (Attribute attr : schema2.attributes) {
            otherNames.add(attr.name);
        }
        schemaNames.retainAll(otherNames);
        if (schemaNames.isEmpty()) {
            return;  // Don't bother if there are no shared attr
        }
        // Rename shared attrs
        for (String attrName : schemaNames) {
            schema1.attributes.get(schema1.getAttributeIndex(attrName)).name = schema1.name + "." + attrName;
            schema2.attributes.get(schema2.getAttributeIndex(attrName)).name = schema2.name + "." + attrName;
        }
    }

    /**
     * Performs a cartesian join on two tables and returns the TableSchema pointing to the
     * table which contains the resulting records
     * @param table_1 The first table
     * @param table_2 The second table
     * @return The TableSchema of the resulting table. This should be dropped after use
     * @throws IOException if an error occurs when creating the cartesian table
     */
    private TableSchema cartesianJoin(TableSchema table_1, TableSchema table_2)  {
        TableSchema larger;
        TableSchema smaller;
        if (table_1.pageCount() >= table_2.pageCount()) {
            larger = table_1.duplicate();
            smaller = table_2.duplicate();
        } else {
            larger = table_2.duplicate();
            smaller = table_1.duplicate();
        }
        // Check for duplicate attr names
        dedupeAttrNames(larger, smaller);
        // Create the joined list of attributes
        ArrayList<Attribute> concatAttr = new ArrayList<>();
        concatAttr.addAll(larger.attributes);
        concatAttr.addAll(smaller.attributes);
        for (Attribute attr : concatAttr) {
            if (attr.primaryKey) {
                attr.primaryKey = false;
                attr.notNull = true;
            }
        }
        // Create the temp table
        TableSchema combinedSchema = null;
        try {
            combinedSchema = storageManager.createTable(storageManager.getTempTableName(), concatAttr);
        } catch (IOException e) {
            System.err.println("Encountered error while creating temp table: " + e);
            return null;
        }
        if (combinedSchema != null) {
            // Block nested loop join
            int largerIndex = 0;
            Page largerPage = storageManager.getPage(larger, 0);
            while (largerPage != null) {
                int smallerIndex = 0;
                Page smallerPage = storageManager.getPage(smaller, 0);
                while (smallerPage != null) {
                    smallerPage = storageManager.getPage(smaller, smallerIndex);
                    for (Record lRec : largerPage.getRecords()) {
                        for (Record rRec : smallerPage.getRecords()) {
                            ArrayList<Object> rowData = new ArrayList<>(lRec.rowData);
                            rowData.addAll(rRec.rowData);
                            storageManager.fastInsert(combinedSchema, new Record(rowData));
                        }
                    }
                    smallerIndex++;
                    smallerPage = storageManager.getPage(smaller, smallerIndex);
                }
                largerIndex++;
                largerPage = storageManager.getPage(larger, largerIndex);
            }
        }
        return combinedSchema;
    }

    /**
     * Takes in the schema of a table and returns a TableSchema containing a subset of its columns
     * @param schema The TableSchema of the table being projected
     * @param attrs The attributes to include in the projection
     * @return The TableSchema of the projection
     */
    private TableSchema projection(TableSchema schema, ArrayList<String> attrs) {
        // Translate and validate parameters
        if (!allUnique(attrs)) {
            System.err.println("Invalid select: duplicate attribute names found - "+String.join(", ", attrs));
            return null;
        }
        int[] attrIndices = new int[attrs.size()];
        for (int i = 0; i < attrs.size(); i++) {
            if (attrs.get(i).equals(",")) {
                continue;
            }
            attrIndices[i] = schema.getAttributeIndex(attrs.get(i));
            if (attrIndices[i] == -1) {
                System.err.println("Unknown attribute '" + attrs.get(i) + "'");
                return null;
            }
        }
        // Create projection schema
        TableSchema projSchema;
        try {
            String projName = storageManager.getTempTableName();
            ArrayList<Attribute> projAttrList = new ArrayList<>();
            for (int i : attrIndices) {
                projAttrList.add(new Attribute(schema.attributes.get(i)));
            }
            projSchema = storageManager.createTable(projName, projAttrList);
        } catch (IOException ioe) {
            System.err.println("Encountered error while cloning table: " + ioe + " : " + ioe.getMessage());
            return null;
        }
        // Fill projection table
        Page currPage = storageManager.getPage(schema, 0);
        int currIndex = 0;
        while (currPage != null) {
            for (Record r : currPage.records) {
                Record projRec = new Record();
                for (int index : attrIndices) {
                    projRec.rowData.add(r.rowData.get(index));
                }
                storageManager.fastInsert(projSchema, projRec);
            }
            currIndex += 1;
            currPage = storageManager.getPage(schema, currIndex);
        }
        return projSchema;
    }

    //endregion

    // ====================================================================================
    //region Parsing ======================================================================
    // ====================================================================================

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
                            (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false"))   // booleans can only be true or false
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
      * @return true if every object of an arraylist is unique, false if there is a duplicate
     */
    private boolean allUnique(ArrayList<String> row) {
        for (int i = 0; i < row.size(); i++) {
            for (int j = i + 1; j < row.size(); j++) {
                if (row.get(i).equals(row.get(j))) {
                    return false;
                }
            }
        }
        return true;
    }

    //endregion

    // ====================================================================================
    //region Printing =====================================================================
    // ====================================================================================

    /// Output constants
    private static final String LEFT_WALL = "| ";
    private static final String RIGHT_WALL = " |";
    private static final String CELL_DIVIDER = " | ";
    private static final char TRUCATION_CHAR = '…';

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
        if (records.isEmpty()) {
            return "";
        }

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

    //endregion

    public void test(ArrayList<String> args) {
        args.removeFirst();
        TableSchema ts = storageManager.getTableSchema("bar");
        Evaluator e = new Evaluator(args, ts);
        int index = 0;
        Page p = storageManager.getPage(ts, 0);
        while (p != null) {
            for (Record r : p.records) {
                if (e.evaluateRecord(r)) {
                    System.out.println(r + " =================================== (TRUE)");
                } else {
                    System.out.println(r);
                }
            }
            index += 1;
            p = storageManager.getPage(ts, index);
        }
    }
}