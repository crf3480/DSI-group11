package tableData;

import java.io.File;
import java.util.ArrayList;

public class TableSchema {

    public String name;
    public int primaryKey = -1;
    public int rootIndex; // The location of page 0 in the table, as a number of pageSize chunks
    public int treeRoot;  // The location of the root of its BPlus tree
    public ArrayList<Integer> numberMap;
    public ArrayList<Attribute> attributes;
    private final String fileDir;
    private int recordCount;
    private int pageCount;
    public final int pageSize;

    /**
     * Creates a TableSchema. This should not be directly called by any classes other than Catalog
     * @param name The name of the table
     * @param rootIndex The index of the first page in the table. `-1` if table has no pages
     * @param attributeArrayList An ArrayList containing the table's attributes
     * @param fileDir The directory which contains the table's Page file
     */
    public TableSchema(String name,
                       int rootIndex,
                       int treeRoot,
                       ArrayList<Integer> numberMap,
                       ArrayList<Attribute> attributeArrayList,
                       String fileDir,
                       int pageCount,
                       int recordCount,
                       int pageSize) {
        this.name = name;
        this.rootIndex = rootIndex;
        this.treeRoot = treeRoot;
        this.numberMap = numberMap;
        this.attributes = attributeArrayList;
        this.fileDir = fileDir;
        this.recordCount = recordCount;
        this.pageCount = pageCount;
        this.pageSize = pageSize;
        // Verify the attribute names are distinct and there is at least one primary key
        ArrayList<String> attributeNames = new ArrayList<>();
        for (int i = 0; i < attributeArrayList.size(); i++) {
            Attribute attribute = attributeArrayList.get(i);
            if (attributeNames.contains(attribute.name)) {
                throw new IllegalArgumentException("Duplicate attribute name: '" + attribute.name + "'");
            }
            attributeNames.add(attribute.name);
            if (attribute.primaryKey) {
                primaryKey = i;
            }
        }
    }

    /**
     * Returns the index of the attribute with a given name
     * @param attributeName The name of the attribute to look for
     * @return The index of the attribute within the schema. If an attribute with that name does not exist, '-1';
     */
    public int getAttributeIndex(String attributeName) {
        // Fetch attribute with the exact name
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).name.equals(attributeName)) {
                return i;
            }
        }
        // If none found, check for search by <tableName>.<attrName>
        if (attributeName.startsWith(name + ".")) {
            return getAttributeIndex(attributeName.substring(name.length() + 1));
        }
        return -1;
    }

    /**
     * returns a list of all attribute names in the table
     */
    public ArrayList<String> getAttributeNames() {
        ArrayList<String> attributeNames = new ArrayList<>();
        for (Attribute attribute : attributes) {
            attributeNames.add(attribute.name);
        }
        return attributeNames;
    }

    /**
     * Returns a clone of the table's Primary Key Attribute
     * @return The primary key attribute for this table
     */
    public Attribute getPK() {
        return new Attribute(attributes.get(primaryKey));
    }

    /**
     * Gets the number of records stored in this table
     * @return The number of records
     */
    public int recordCount() {
        return recordCount;
    }

    /**
     * Increments the number of records by one<br>
     * NOTE: Public access to recordCount or incrementing by more than 1 is <b>deliberately</b>
     * forbidden. All modifications of a table's records occur one at a time, and should be
     * tracked as such.
     */
    public void incrementRecordCount() {
        recordCount++;
    }

    /**
     * Increments the number of records by one<br>
     * NOTE: Public access to recordCount or decrementing by more than 1 is <b>deliberately</b>
     * forbidden. All modifications of a table's records occur one at a time, and should be
     * tracked as such.
     */
    public void decrementRecordCount() {
        if (recordCount <= 0) {
            System.err.println("ERROR: Attempted to decrement record count of zero");
            return;
        }
        recordCount--;
    }

    /**
     * Gets the number of pages stored in this table
     * @return The number of pages
     */
    public int pageCount() {
        return pageCount;
    }

    /**
     * Increments the number of pages by one<br>
     * NOTE: Public access to pageCount or incrementing by more than 1 is <b>deliberately</b>
     * forbidden. All modifications of a table's pages occur one at a time, and should be
     * tracked as such.
     */
    public void incrementPageCount() {
        pageCount++;
    }

    /**
     * Increments the number of pages by one<br>
     * NOTE: Public access to pageCount or decrementing by more than 1 is <b>deliberately</b>
     * forbidden. All modifications of a table's pages occur one at a time, and should be
     * tracked as such.
     */
    public void decrementPageCount() {
        if (pageCount <= 0) {
            System.err.println("ERROR: Attempted to decrement page count of zero");
            return;
        }
        pageCount--;
    }

    /**
     * Returns a File object matching the page file for this table
     * @return The table's File object
     */
    public File tableFile(){
        return new File(fileDir + name + ".bin");
    }

    /**
     * Returns a File object matching the BPlusTree file for this table
     * @return The BPlusTree's File object
     */
    public File indexFile(){
        return new File(fileDir + name + ".bpt");
    }

    /**
     * Returns the number of attributes in this table that are can be null
     * @return The number of nullable attributes
     */
    public int nullableAttributes() {
        int count = 0;
        for (Attribute attribute : attributes) {
            if (!attribute.notNull && !attribute.primaryKey) {
                count += 1;
            }
        }
        return count;
    }


    /**
     * Returns the file offset of the page with a given page number. This is O(n).
     * When possible, use getPageNumber() instead
     * @param pageNumber The page number to look for
     * @return The offset into the page file for this page number in this table
     */
    public int getIndex(int pageNumber) {
        return numberMap.lastIndexOf(pageNumber);
    }

    /**
     * Gets the page number of the page at a given index
     * @param pageIndex The offset being searched for
     * @return The page number corresponding to the given offset; `-1` if page does not exist
     */
    public int getPageNumber(int pageIndex) {
        if (pageIndex < 0 || pageIndex >= numberMap.size()) {
            return -1;
        }
        return numberMap.get(pageIndex);
    }

    /**
     * Inserts a new page of a given number into a table's numberMap. All existing pages
     * with a greater number will be incremented by one to preserve order
     * @param pageNumber The page number being added
     * @param atIndex The index the page was inserted at
     */
    public void insertPage(int pageNumber, int atIndex) {
        // Increment all pages of equal or greater pageNumber
        for (int i = 0; i < numberMap.size(); i++) {
            if (numberMap.get(i) >= pageNumber) {
                numberMap.set(i, numberMap.get(i) + 1);
            }
        }
        // Insert pageNumber
        numberMap.add(atIndex, pageNumber);
    }

    /**
     * Removes the mapping of a page's number from table's numberMap. All remaining pages
     * with a greater number will be decremented by one to preserve order
     * @param pageNumber The page number being removed
     */
    public void removePage(int pageNumber) {
        // Increment all pages of equal or greater pageNumber
        for (int i = 0; i < numberMap.size(); i++) {
            int mappedNumber = numberMap.get(i);
            if (mappedNumber == pageNumber) {
                numberMap.set(i, -1);  // Remove number
            } else if (mappedNumber > pageNumber) {
                numberMap.set(i, mappedNumber + 1);  // Decrement subsequent pages
            }
        }
    }

    /**
     * Returns the smallest pageOffset that is not currently occupied by a page,
     * or `-1` if no such gap exists
     * @return The smallest unoccupied pageIndex; `-1` if page file has no gaps
     */
    public int getFirstPageGap() {
        return numberMap.indexOf(-1);
    }

    /**
     * Creates a deep copy of this TableSchema
     * @return The duplicated TableSchema
     */
    public TableSchema duplicate() {
        return this.duplicate(name);
    }

    /**
     * Creates a deep copy of this TableSchema with a different name
     * @return The duplicated TableSchema
     */
    public TableSchema duplicate(String tableName) {
        ArrayList<Integer> duplicateNumberMap = new ArrayList<>(numberMap);
        ArrayList<Attribute> duplicateAttributes = new ArrayList<>();
        for (Attribute attr : attributes) {
            duplicateAttributes.add(new Attribute(attr));
        }
        return new TableSchema(tableName, rootIndex, treeRoot, duplicateNumberMap, duplicateAttributes, fileDir, pageCount, recordCount, pageSize);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Attribute attribute : attributes) {
            sb.append('\t');
            sb.append(attribute.name);
            sb.append(':');
            sb.append(attribute.type);
            if (attribute.type == AttributeType.CHAR || attribute.type == AttributeType.VARCHAR) {
                sb.append('(');
                sb.append(attribute.length);
                sb.append(')');
            }
            if (attribute.primaryKey) { sb.append(" primary key"); }
            if (attribute.unique) { sb.append(" unique"); }
            if (attribute.notNull) { sb.append(" notnull"); }
            sb.append('\n');
        }

        return sb.toString();
    }
}