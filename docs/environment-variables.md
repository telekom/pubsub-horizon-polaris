<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

# Environment Variables

| Name                                        | Default                   | Description                                                                                                                |
|---------------------------------------------|---------------------------|----------------------------------------------------------------------------------------------------------------------------|
| POLARIS_MAX_TIMEOUT                         | 30000                     | Maximum time to wait for a response from the customer's endpoint.                                                          |
| POLARIS_MAX_CONNECTIONS                     | 100                       | Maximum number of simultaneous connections to customers' endpoints.                                                        |
| POLARIS_DELIVERING_STATES_OFFSET_MINS       | 15                        | Only load MessageStates with a time < (now - deliveringStates-offset-mins).                                                |
| POLARIS_POLLING_INTERVAL_MS                 | 30000                     | Interval in milliseconds for Polaris to periodically poll circuit breaker messages and events in DELIVERING/FAILED status. |
| POLARIS_POLLING_BATCH_SIZE                  | 10                        | Number of events to be polled in each batch during the periodic polling process.                                           |
| POLARIS_PICKING_TIMEOUT_MS                  | 5000                      | Timeout in milliseconds for Polaris to wait for an event to be picked for redelivery.                                      |
| POLARIS_REQUEST_COOLDOWN_RESET_MINS         | 90                        | Needs to be more than 60 because 60 mins can be the maximum cooldown on loop.                                              |
| POLARIS_REQUEST_THREADPOOL_SIZE             | 50                        | Maximum number of threads in the thread pool for health check requests.                                                    |
| POLARIS_REQUEST_DELAY_MINS                  | 5                         | Delay in minutes before starting the health check request after a failed attempt.                                          |
| POLARIS_SUCCESSFUL_STATUS_CODES             | 200,201,202,204           | Comma-separated list of HTTP status codes considered as successful for health checks.                                      |
| POLARIS_SUBCHECK_THREADPOOL_MAX_SIZE        | 50                        | Maximum number of threads in the thread pool for subscription checks. (will be set to Integer.Max if set to "")                                                     |
| POLARIS_SUBCHECK_THREADPOOL_CORE_SIZE       | 50                        | Core number of threads in the thread pool for subscription checks.                                                         |
| POLARIS_SUBCHECK_THREADPOOL_QUEUE_CAPACITY  | 50                        | Capacity of the queue used by the thread pool for subscription checks. (will be set to Integer.Max if set to "")           |
| POLARIS_REPUBLISH_THREADPOOL_MAX_SIZE       | 50                        | Maximum number of threads in the thread pool for republishing events. (will be set to Integer.Max if set to "")                                                     |
| POLARIS_REPUBLISH_THREADPOOL_CORE_SIZE      | 50                        | Core number of threads in the thread pool for republishing events.                                                         |
| POLARIS_REPUBLISH_THREADPOOL_QUEUE_CAPACITY | 50                        | Capacity of the queue used by the thread pool for republishing events. (will be set to Integer.Max if set to "")           |
| POLARIS_REPUBLISH_BATCH_SIZE                | 20                        | Number of events to be republished in each batch during the republishing process.                                          |
| POLARIS_REPUBLISHING_TIMEOUT_MS             | 5000                      | Timeout in milliseconds for Polaris to wait for an event to be republished.                                                |
| POLARIS_KAFKA_BROKERS                       | kafka:9092,localhost:9092 | Kafka brokers used by Polaris for communication.                                                                           |
| POLARIS_KAFKA_LINGER_MS                     | 5                         | How long Kafka waits for other records before transmitting the batch.                                                      |
| POLARIS_KAFKA_ACKS                          | 1                         | Number of acknowledgments the producer requires the leader to receive.                                                     |
| POLARIS_KAFKA_GROUPID                       | plunger                   | Kafka consumer group ID used by Polaris.                                                                                   |
| POLARIS_KAFKA_COMPRESSION_ENABLED           | false                     | Whether events sent to Kafka should be compressed.                                                                         |
| POLARIS_KAFKA_COMPRESSION_TYPE              | none                      | The compression type used to compress events sent to Kafka.                                                                |
| POLARIS_APP_NAME                            | horizon-polaris           | Name of the Polaris application.                                                                                           |
| POLARIS_INFORMER_NAMESPACE                  | integration               | Namespace used by Polaris to inform about changes.                                                                         |
