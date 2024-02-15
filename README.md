<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

<h1 align="center">Horizon Polaris</h1>

<p align="center">
  <img src="docs/img/Horizon.svg" alt="polaris-logo" width="120px" height="120px"/>
  <br>
  <em>Horizon component for circuit-breaking and automated event redelivery</em>
  <br>
</p>
<p>
  <a href="#overview">Overview</a>
  ·
  <a href="#development">Development</a>
  ·
  <a href="#next-steps">Next Steps</a>
  ·
  <a href="#documentation">Documentation</a>
  ·
  <a href="#changelog">Changelog</a>
</p>

- [Overview](#overview)
- [Development](#development)
    - [Prerequisites](#prerequisites)
    - [Setting Up for Development](#setting-up-for-development)
    - [Operational Information](#operational-information)
- [Next Steps](#next-steps)
- [Documentation](#documentation)
    - [Environment Variables](docs/env-docs.md)
    - [Architecture](docs/architecture.md)
    - [Getting Started](docs/getting-started.md)
- [Changelog](#changelog)

<hr>

## Overview

Horizon Polaris, formerly known as Plunger, serves as the circuit breaker within the Horizon ecosystem. It ensures the redelivery of failed events by periodically checking the availability of a customer's endpoint using HEAD or GET requests. When the endpoint becomes available, all events for that customer and endpoint in the WAITING status are sent to Kafka, where Comet picks them up for redelivery.

## Development

### Prerequisites

To test changes locally, ensure the following prerequisites are met:

1. Have a Kubernetes config at `${user.home}/.kube/config.laptop-awsd-live-system` pointing to a non-production cluster.
   Ensure a namespace is configured as in `kubernetes.informer.namespace` and a CustomResource `subscriptions.subscriber.horizon.telekom.de`.
   The resource definition can be found in the [Horizon Essentials Helm Chart](https://gitlab.devops.telekom.de/dhei/teams/pandora/argocd-charts/horizon-3.0/essentials/-/tree/main?ref_type=heads)

### Setting Up for Development

Follow these steps to set up Horizon Polaris for local development:

#### 1. Clone the Repository

```bash
git clone [repository-url]
cd polaris
```

2. Install Dependencies
```bash
./gradlew build
```

3. Start docker-compose
```bash
docker-compuse up -d
```

4. Run Locally
```bash
./gradlew bootRun --args='--spring.profiles.active=dev'
```
This command will start Horizon Polaris in development mode.

## Operational Information
Polaris shares a Hazelcast cache with Comet, named the circuit breaker cache. When Comet is unable to deliver an event, Polaris adds an entry to this cache. Polaris periodically polls this cache, picking up entries with the circuit breaker status OPEN. It then hashes the callback URL, calculates its pod index, and handles the circuit breaker message. If a circuit breaker message is already assigned to another pod, Polaris checks if the pod is still alive. If it is, Polaris skips handling the message.

## Next Steps
Explore the project, make changes, and contribute to the Horizon ecosystem.

## Documentation
- [Environment Variables](docs/env-docs)
- [Architecture](docs/architecture.md)

[//]: # ([Getting Started]&#40;docs/getting-started&#41;)

## Code of Conduct

This project has adopted the [Contributor Covenant](https://www.contributor-covenant.org/) in version 2.1 as our code of conduct. Please see the details in our [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). All contributors must abide by the code of conduct.

By participating in this project, you agree to abide by its [Code of Conduct](./CODE_OF_CONDUCT.md) at all times.

## Licensing

This project follows the [REUSE standard for software licensing](https://reuse.software/).
Each file contains copyright and license information, and license texts can be found in the [./LICENSES](./LICENSES) folder. For more information visit https://reuse.software/.

### REUSE

For a comprehensive guide on how to use REUSE for licensing in this repository, visit https://telekom.github.io/reuse-template/.   
A brief summary follows below:

The [reuse tool](https://github.com/fsfe/reuse-tool) can be used to verify and establish compliance when new files are added.

For more information on the reuse tool visit https://github.com/fsfe/reuse-tool.