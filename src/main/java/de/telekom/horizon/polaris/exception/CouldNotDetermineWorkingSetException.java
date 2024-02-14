// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.exception;

public class CouldNotDetermineWorkingSetException extends HorizonPolarisException {
    public CouldNotDetermineWorkingSetException(String message, Throwable e) {
        super(message, e);
    }

    public CouldNotDetermineWorkingSetException(String message) {
        super(message);
    }
}
