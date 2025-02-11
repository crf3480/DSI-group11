package Parsers.Helpers;

public class Attribute {
    public String name;
    public String type;
    public ConstraintTypes constraint;

    /**
     * Creates a new attribute that defines an attribute with the following constraints
     * @param name Constraint Name
     * @param type Type of Value
     * @param constraint Type of Constraint (PRIMARY_KEY, NOT_NULL, UNIQUE)
     */
    public Attribute(String name, String type, ConstraintTypes constraint) {
        this.name = name;
        this.type = type;
        this.constraint = constraint;
    }
}

