package tableData;

public enum AttributeType {
    INT, DOUBLE, CHAR, VARCHAR, BOOLEAN;

    public static AttributeType fromString(String typeName) throws IllegalArgumentException {
        typeName = typeName.toUpperCase();
        return switch (typeName) {
            case "INTEGER" -> INT;
            case "DOUBLE" -> DOUBLE;
            case "CHAR" -> CHAR;
            case "VARCHAR" -> VARCHAR;
            case "BOOLEAN" -> BOOLEAN;
            default -> throw new IllegalArgumentException("Invalid attribute type: " + typeName);
        };
    }

    @Override
    public String toString() {
        return switch (this) {
            case INT -> "int";
            case DOUBLE -> "double";
            case CHAR -> "char";
            case VARCHAR -> "varchar";
            case BOOLEAN -> "boolean";
        };
    }
}
