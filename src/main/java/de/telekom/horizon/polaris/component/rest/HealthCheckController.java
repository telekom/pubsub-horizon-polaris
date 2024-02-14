package de.telekom.horizon.polaris.component.rest;


import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.model.HealthCheckData;
import lombok.AllArgsConstructor;
import org.springframework.data.crossstore.ChangeSetPersister;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Collection;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@AllArgsConstructor
@RestController()
@RequestMapping("health-checks")
public class HealthCheckController {
    private HealthCheckCache healthCheckCache;

    /**
     * Retrieves all health check data entries.
     *
     * @return Collection of {@link HealthCheckData} entries.
     */
    @GetMapping()
    public Collection<HealthCheckData> getAll() {
        return healthCheckCache.getAll();
    }

    /**
     * Retrieves health check data entries for a specific callback URL.
     *
     * @param callbackUrl The callback URL to retrieve health check data for.
     * @return Collection of {@link HealthCheckData} entries for the specified callback URL.
     */
    @GetMapping(params = {"callbackUrl"})
    public Collection<HealthCheckData> getForCallbackUrl(@RequestParam String callbackUrl) {
        var healthCheckDatas = new ArrayList<HealthCheckData>();

        var oHealthCheckDataHEAD = healthCheckCache.get(callbackUrl, HttpMethod.HEAD);
        var oHealthCheckDataGET = healthCheckCache.get(callbackUrl, HttpMethod.GET);

        oHealthCheckDataHEAD.ifPresent(healthCheckDatas::add);
        oHealthCheckDataGET.ifPresent(healthCheckDatas::add);

        if(healthCheckDatas.isEmpty()) {
            throw new ResponseStatusException(NOT_FOUND, "Unable to find resource");
        }

        return healthCheckDatas;
    }

    /**
     * Retrieves health check data entry for a specific callback URL and HTTP method.
     *
     * @param callbackUrl The callback URL to retrieve health check data for.
     * @param httpMethod  The HTTP method to retrieve health check data for (should be HEAD or GET).
     * @return {@link HealthCheckData} entry for the specified callback URL and HTTP method.
     * @throws ResponseStatusException If the HTTP method is not HEAD or GET (HTTP 400), or if the resource is not found (HTTP 404).
     */
    @GetMapping(params = {"callbackUrl", "httpMethod"})
    public HealthCheckData getForCallbackUrlAndHttpMethod(@RequestParam String callbackUrl, @RequestParam HttpMethod httpMethod) {
        if(!HttpMethod.GET.equals(httpMethod) && !HttpMethod.HEAD.equals(httpMethod)) {
            throw new ResponseStatusException(BAD_REQUEST, "HttpMethods needs to be HEAD or GET!");
        }

        var oHealthCheckDataHEAD = healthCheckCache.get(callbackUrl, httpMethod);
        if(oHealthCheckDataHEAD.isEmpty()) {
            throw new ResponseStatusException(NOT_FOUND, "Unable to find resource");
        }

        return oHealthCheckDataHEAD.get();
    }

}
