// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.util;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.matching.AnythingPattern;
import org.springframework.http.HttpStatus;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;

public class WiremockStubs {

    public static void stubOidc(WireMockExtension wireMockServer) {
        wireMockServer.stubFor(
                post("/oidc").withRequestBody(new AnythingPattern()).willReturn(
                        aResponse()
                                .withStatus(HttpStatus.OK.value())
                                .withBody("""
                                        {
                                            "expires_in": 1000,
                                            "access_token": "foobar"
                                        }
                                        """)
                )
        );
    }
}
