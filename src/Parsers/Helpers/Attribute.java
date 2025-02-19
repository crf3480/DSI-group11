package Parsers.Helpers;

public class Attribute {
    public String name;
    public AttributeType type;
    public boolean primaryKey;
    public boolean notNull;
    public boolean unique;

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


}

