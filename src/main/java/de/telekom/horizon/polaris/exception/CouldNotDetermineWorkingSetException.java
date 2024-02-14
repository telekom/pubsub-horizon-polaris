package de.telekom.horizon.polaris.exception;

public class CouldNotDetermineWorkingSetException extends HorizonPolarisException {
    public CouldNotDetermineWorkingSetException(String message, Throwable e) {
        super(message, e);
    }

    public CouldNotDetermineWorkingSetException(String message) {
        super(message);
    }
}
