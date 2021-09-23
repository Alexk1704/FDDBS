package fdbs.sql;

public class FedException extends Exception {

    private static final long serialVersionUID = 1L;

    public FedException(final Throwable cause) {
        super(cause);
    }

    public FedException(String message, Throwable cause) {
        super(message, cause);
    }

    public FedException(String message) {
        super(message);
    }   
}
