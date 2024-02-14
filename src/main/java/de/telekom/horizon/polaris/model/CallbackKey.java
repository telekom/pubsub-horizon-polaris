package de.telekom.horizon.polaris.model;

import org.springframework.http.HttpMethod;

public record CallbackKey(String callbackUrl, HttpMethod httpMethod) {
}
