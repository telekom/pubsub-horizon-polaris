package de.telekom.horizon.polaris.model;

import java.util.List;

public record CloseCircuitBreakersRequest(List<String> subscriptionIds) {
}
