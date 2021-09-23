package fdbs.sql.resolver;

public class StatementResolverException extends Exception {

    /**
     * constructor
     *
     * @param message
     */
    public StatementResolverException(String message) {
        super(message);
    }

    /**
     * constructor
     *
     * @param message
     * @param cause, throwable exception
     */
    public StatementResolverException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * constructor
     *
     * @param cause A throwable exception.
     */
    public StatementResolverException(Throwable cause) {
        super(cause);
    }
}
