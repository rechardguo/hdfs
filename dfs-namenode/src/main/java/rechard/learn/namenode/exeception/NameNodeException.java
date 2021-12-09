package rechard.learn.namenode.exeception;

/**
 * @author Rechard
 **/
public class NameNodeException extends Exception {

    public NameNodeException() {
    }

    public NameNodeException(String message) {
        super(message);
    }

    public NameNodeException(Throwable ex) {
        super(ex);
    }
}
