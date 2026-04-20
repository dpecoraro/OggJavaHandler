package oracle.goldengate.util;

public class GGException extends Exception {
    private static final long serialVersionUID = 1L;

    public GGException() {
        super();
    }

    public GGException(String message) {
        super(message);
    }

    public GGException(String message, Throwable cause) {
        super(message, cause);
    }

    public GGException(Throwable cause) {
        super(cause);
    }
}
