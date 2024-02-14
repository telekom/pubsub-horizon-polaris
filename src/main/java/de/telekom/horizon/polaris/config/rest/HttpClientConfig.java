package de.telekom.horizon.polaris.config.rest;

import de.telekom.horizon.polaris.config.PolarisConfig;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HttpClientConfig {

    private final PolarisConfig polarisConfig;
    @Autowired
    public HttpClientConfig(PolarisConfig polarisConfig) {
        this.polarisConfig = polarisConfig;
    }

    @Bean
    public PoolingHttpClientConnectionManager poolingHttpClientConnectionManager() {
        var connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(polarisConfig.getMaxConnections());
        connectionManager.setDefaultMaxPerRoute(polarisConfig.getMaxConnections());
        return connectionManager;
    }

    @Bean
    public RequestConfig requestConfig() {
        return RequestConfig.custom()
                .setConnectionRequestTimeout((int) polarisConfig.getMaxTimeout())
                .setConnectTimeout((int) polarisConfig.getMaxTimeout())
                .setSocketTimeout((int) polarisConfig.getMaxTimeout())
                .build();
    }

    @Bean
    public CloseableHttpClient httpClient(PoolingHttpClientConnectionManager poolingHttpClientConnectionManager, RequestConfig requestConfig) {

        return HttpClientBuilder
                .create()
                .disableCookieManagement()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();
    }
}
