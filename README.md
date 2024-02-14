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

## Changelog
See [CHANGELOG.md](CHANGELOG.md).

## Related Projects

- [Starlight](https://gitlab.devops.telekom.de/dhei/teams/pandora/products/horizon/starlight)
- [Galaxy](https://gitlab.devops.telekom.de/dhei/teams/pandora/products/horizon/galaxy) 
- [Comet](https://gitlab.devops.telekom.de/dhei/teams/pandora/products/horizon/comet)
- [Pulsar](https://gitlab.devops.telekom.de/dhei/teams/pandora/products/horizon/pulsar)
- [Polaris](https://gitlab.devops.telekom.de/dhei/teams/pandora/products/horizon/Polaris) (*you are here*)