package sirius.dbaccess;

/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class NotAvailableConnectionException extends Exception {

    private static final long serialVersionUID = 1L;

    public NotAvailableConnectionException() {

    }

    public NotAvailableConnectionException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    public NotAvailableConnectionException(Throwable cause) {
        super(cause);
    }

    public NotAvailableConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

}
