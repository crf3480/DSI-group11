package where;

/**
 * An exception thrown when a where clause parses an invalid operator
 */
public class InvalidOperatorException extends IllegalArgumentException {
    public InvalidOperatorException(String message) {
        super(message);
    }
}
