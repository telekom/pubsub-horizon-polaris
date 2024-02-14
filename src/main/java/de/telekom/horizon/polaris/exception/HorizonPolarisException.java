package de.telekom.horizon.polaris.exception;


import de.telekom.eni.pandora.horizon.common.exception.HorizonException;

public abstract class HorizonPolarisException extends HorizonException {

    protected HorizonPolarisException(String message, Throwable e) {
        super(message, e);
    }

    protected HorizonPolarisException(String message) {
        super(message);
    }
}
