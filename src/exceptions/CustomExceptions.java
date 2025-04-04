package exceptions;

public class CustomExceptions extends RuntimeException {
    public CustomExceptions(String message) {
        super(message);
    }

    public static class IncompatibleTypeComparisonException extends RuntimeException {
        public IncompatibleTypeComparisonException(String message) {
            super(message);
        }
    }

    /**
     * An exception thrown when a where clause parses an invalid operator
     */
    public static class InvalidOperatorException extends IllegalArgumentException {
        public InvalidOperatorException(String message) {
            super(message);
        }
    }

    public static class WhereSyntaxError extends RuntimeException {
        public WhereSyntaxError(String message) {
            super(message);
        }
    }

    /**
     * An exception thrown when a user inputs an attribute that does not exist
     */
    public static class InvalidAttributeException extends IllegalArgumentException {
        public InvalidAttributeException(String message) { super(message); }
    }

    /**
     * An exception thrown when a user inputs a table name that does not exist
     */
    public static class InvalidTableException extends IllegalArgumentException {
        public InvalidTableException(String message) { super(message); }
    }
}