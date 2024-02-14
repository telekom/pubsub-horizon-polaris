// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.exception;

public class CouldNotPickMessageException extends HorizonPolarisException {

    public CouldNotPickMessageException(String message, Throwable e) {
        super(message, e);
    }

    public CouldNotPickMessageException(String message) {
        super(message);
    }
}
