package de.telekom.horizon.polaris.model;

import java.util.List;

public record CloseCircuitBreakersResponse(List<String> subscriberIdsNotFoundInSubscriptionCache) {
}
