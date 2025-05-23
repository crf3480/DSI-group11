package tableData;

public class Attribute {
    public String name;
    public AttributeType type;
    public boolean primaryKey;
    public boolean notNull;
    public boolean unique;
    public Object defaultValue;

    /// The number of bytes this attribute takes up
    public int length;

    /**
     * Creates a new attribute that defines an attribute with the following constraints
     * @param name Constraint Name
     * @param type Type of Value
     * @param primaryKey Whether this attribute is a primary key
     * @param notNull Whether this attribute is allowed to be null
     * @param unique Whether this attribute must be unique in its relation
     * @param length The byte length of this attribute. For fixed length attributes, this value will be ignored
     */
    public Attribute(String name, AttributeType type, boolean primaryKey, boolean notNull, boolean unique, int length) {
        this.name = name;
        this.type = type;
        this.primaryKey = primaryKey;
        this.notNull = notNull;
        this.unique = unique;
        this.defaultValue = null;

        switch(type){
            case DOUBLE:
                this.length = Double.BYTES;
                break;
            case INT:
                this.length = Integer.BYTES;
                break;
            case BOOLEAN:
                this.length = 1;
                break;

            default:
                this.length = length;
                break;
        }
    }

    /**
     * Creates a new attribute that defines an attribute with the following constraints
     * @param name Constraint Name
     * @param type Type of Value
     * @param primaryKey Whether this attribute is a primary key
     * @param notNull Whether this attribute is allowed to be null
     * @param unique Whether this attribute must be unique in its relation
     * @param length The byte length of this attribute. For fixed length attributes, this value will be ignored
     * @param defaultValue The default value to fill in if one is not provided
     */
    public Attribute(String name, AttributeType type, boolean primaryKey, boolean notNull, boolean unique, int length, Object defaultValue) {
        this.name = name;
        this.type = type;
        this.primaryKey = primaryKey;
        this.notNull = notNull || primaryKey;
        this.unique = unique;

        // Throw an error if defaultValue is null and attribute cannot be null
        if (defaultValue == null && notNull){
            System.err.print("Warning: Attribute is not null but default value is null");
            //TODO: Actually throw an error
        }

        this.defaultValue = defaultValue;

        switch(type){
            case DOUBLE:
                this.length = 8;
                break;
            case INT:
                this.length = 4;
                break;
            case BOOLEAN:
                this.length = 1;
                break;

            default:
                this.length = length;
                break;
        }
    }

    /**
     * Creates a deep copy of an Attribute
     * @param attr The attribute copy
     */
    public Attribute(Attribute attr) {
        this.name = attr.name;
        this.type = attr.type;
        this.primaryKey = attr.primaryKey;
        this.notNull = attr.notNull;
        this.unique = attr.unique;
        this.length = attr.length;
        this.defaultValue = attr.defaultValue;
    }

    /**
     * Checks if this attribute is allowed to be assigned null
     * @return 'true' if this attribute accepts a null assignment, 'false' otherwise
     */
    public boolean allowsNull() {
        return !primaryKey && !notNull;
    }

    /**
     * Returns the number of bytes this attribute will take up when stored as a string
     * @return `length` if the attribute is a non-string type; `length + 1` for string types
     */
    public int byteLength() {
        if (this.type == AttributeType.VARCHAR || this.type == AttributeType.CHAR) {
            return length + 1;
        }
        return length;
    }
}

