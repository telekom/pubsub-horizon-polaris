// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.model;

import java.util.List;

public record CloseCircuitBreakersResponse(List<String> subscriberIdsNotFoundInSubscriptionCache) {
}
