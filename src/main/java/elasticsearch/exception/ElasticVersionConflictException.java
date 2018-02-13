package elasticsearch.exception;

/**
 * Created by I319603 on 10/25/2016.
 */
public class ElasticVersionConflictException extends RuntimeException {
    public ElasticVersionConflictException(String message) {
        super(message);
    }

    public ElasticVersionConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
