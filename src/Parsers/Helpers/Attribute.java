package Parsers.Helpers;

public class Attribute {
    public String name;
    public AttributeType type;
    public ConstraintTypes constraint;

    public int length;

    /**
     * Creates a new attribute that defines an attribute with the following constraints
     * @param name Constraint Name
     * @param type Type of Value
     * @param constraint Type of Constraint (PRIMARY_KEY, NOT_NULL, UNIQUE)
     */
    public Attribute(String name, AttributeType type, ConstraintTypes constraint) {
        this.name = name;
        this.type = type;
        this.constraint = constraint;

        switch(type){
            case INT:
                length = 32;
                break;

            default:
                break;
        }
    }

    // TODO: This roughly works for varchar max len (only asking when we have varchar, can be done better..
    public Attribute(String name, AttributeType type, ConstraintTypes constraint, int maxlength) {
        this.name = name;
        this.type = type;
        this.constraint = constraint;
        this.length = length;
    }


}

