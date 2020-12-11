package com.lee.cache.exception;

public class CacheCloseException extends CacheException {

    public CacheCloseException() {
        super();
    }

    public CacheCloseException(String message) {
        super(message);
    }

    public CacheCloseException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheCloseException(Throwable cause) {
        super(cause);
    }

    protected CacheCloseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
