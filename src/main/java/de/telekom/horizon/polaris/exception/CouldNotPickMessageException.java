package de.telekom.horizon.polaris.exception;

public class CouldNotPickMessageException extends HorizonPolarisException {

    public CouldNotPickMessageException(String message, Throwable e) {
        super(message, e);
    }

    public CouldNotPickMessageException(String message) {
        super(message);
    }
}
