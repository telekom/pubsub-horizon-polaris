# Copyright 2024 Deutsche Telekom IT GmbH
#
# SPDX-License-Identifier: Apache-2.0

ARG DOCKER_BASE_IMAGE=azul/zulu-openjdk-alpine:21-jre
FROM ${DOCKER_BASE_IMAGE}

WORKDIR app

COPY build/libs/polaris.jar app.jar

EXPOSE 8080

CMD ["java", "-jar", "app.jar"]