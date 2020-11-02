package gxc.datasource.pool.exception;

/**
 * @author GongXincheng
 * @date 2020/11/2 14:30
 */
public class ReadConfigException extends RuntimeException {

    public ReadConfigException() {
    }

    public ReadConfigException(String message) {
        super(message);
    }

    public ReadConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReadConfigException(Throwable cause) {
        super(cause);
    }
}
