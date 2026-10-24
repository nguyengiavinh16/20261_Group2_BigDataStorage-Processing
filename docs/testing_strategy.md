# Testing Strategy

## Unit tests
Test schema inspection, configuration parsing, feature transformations, threshold
decisions, stable identifiers, and malformed input handling without external services.

## Integration tests
Test HDFS/Spark, Kafka, MongoDB, API, and dashboard connections only when the
corresponding services are available. Mark service-dependent tests separately.

## System tests
Replay a bounded number of records through Kafka and Spark, verify prediction and
alert documents in MongoDB, and query the stored results through the API.

## Reliability tests
Test duplicate, malformed, delayed, and out-of-order events; restart streaming
with the same checkpoint; document the observed recovery behavior.

## Evidence policy
Record command, environment, input size, exit status, and output. Do not mark a
test passed unless it was actually executed successfully.
