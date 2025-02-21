package tableData;

public enum AttributeType {
    INT, DOUBLE, CHAR, VARCHAR, BOOLEAN;

    public static AttributeType fromString(String typeName) throws IllegalArgumentException {
        typeName = typeName.toUpperCase();
        return switch (typeName) {
            case "INT" -> INT;
            case "DOUBLE" -> DOUBLE;
            case "CHAR" -> CHAR;
            case "VARCHAR" -> VARCHAR;
            case "BOOLEAN" -> BOOLEAN;
            default -> throw new IllegalArgumentException("Invalid attribute type: " + typeName);
        };
    }
}
