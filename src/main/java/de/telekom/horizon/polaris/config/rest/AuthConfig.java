package de.telekom.horizon.polaris.config.rest;

import de.telekom.eni.pandora.horizon.auth.OAuth2TokenCache;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Setter
@Configuration
@ConfigurationProperties(prefix = "polaris.oidc")
public class AuthConfig {

    private String tokenUri;
    private String clientId;
    private String clientSecret;

    @Bean
    public OAuth2TokenCache customOauth2Filter() {
        return new OAuth2TokenCache(tokenUri, clientId, clientSecret);
    }
}
