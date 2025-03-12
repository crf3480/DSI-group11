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

    /**
     * Parses a String and returns
     * @param s The string to parse
     * @return An object containing the parsed value
     * @throws IllegalArgumentException If the provided string cannot be converted to this type
     */
    public Object parseString(String s) throws IllegalArgumentException {
        try {
            return switch (this) {
                case INT -> Integer.parseInt(s);
                case BOOLEAN -> Boolean.parseBoolean(s);
                case DOUBLE -> Double.parseDouble(s);
                case CHAR, VARCHAR -> {
                    if (s.charAt(0) != '"' || s.charAt(s.length() - 1) != '"') {
                        throw new IllegalArgumentException("String values must be wrapped in quotes.");
                    }
                    yield s.substring(1, s.length() - 1);
                }
            };
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid default value `" + s + "` for type " + this);
        }
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
