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
}