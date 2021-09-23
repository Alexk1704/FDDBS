package fdbs.sql.semantic;

/**
 * A semantic validation exception.
 */
public class SemanticValidationException extends Exception {
    
    /**
     * The Constructor for semantic validation exception.
     * @param message The message for this exception.
     */
    public SemanticValidationException(String message) {
        super(message);
    }

    /**
     * The Constructor for semantic validation exception.
     * @param message The message for this exception.
     * @param cause A throwable exception.
     */
    public SemanticValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * The Constructor for semantic validation exception.
     * @param cause A throwable exception.
     */
    public SemanticValidationException(Throwable cause) {
        super(cause);
    }
}
