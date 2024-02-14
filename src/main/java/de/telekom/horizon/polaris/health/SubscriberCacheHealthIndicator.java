package de.telekom.horizon.polaris.health;

import de.telekom.eni.pandora.horizon.kubernetes.InformerStoreInitHandler;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class SubscriberCacheHealthIndicator implements HealthIndicator {

    private final InformerStoreInitHandler informerStoreInitHandler;

    public SubscriberCacheHealthIndicator(InformerStoreInitHandler informerStoreInitHandler) {
        this.informerStoreInitHandler = informerStoreInitHandler;
    }

    @Override
    public Health health() {
        Health.Builder status = Health.up();

        if (!informerStoreInitHandler.isFullySynced()) {
            status = Health.down();
        }

        return status.withDetails(informerStoreInitHandler.getInitalSyncedStats()).build();
    }
}