package sirius.zk;

/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class ZookeeperException extends Exception {

    private static final long serialVersionUID = 1L;

    public ZookeeperException(String msg) {
        super(msg);
    }

    public ZookeeperException(String msg, Throwable t) {
        super(msg, t);
    }

    public ZookeeperException(Throwable cause) {
        super(cause);
    }
}
