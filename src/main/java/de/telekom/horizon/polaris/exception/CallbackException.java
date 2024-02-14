package de.telekom.horizon.polaris.exception;

import lombok.Getter;

@Getter
public class CallbackException extends HorizonPolarisException {

    private final int statusCode;

    public CallbackException(String message) {
        super(message);
        this.statusCode = 500;
    }

    public CallbackException(String message, Throwable t) {
        super(message, t);
        this.statusCode = 500;
    }

    public CallbackException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public CallbackException(String message, Throwable t, int statusCode) {
        super(message, t);
        this.statusCode = statusCode;
    }
}
