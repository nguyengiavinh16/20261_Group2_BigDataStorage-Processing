# API Specification (planned for Week 5)

Planned endpoints:
- `GET /health`: service health.
- `GET /metrics/summary`: counts and rates computed from stored records.
- `GET /alerts`: paginated alerts with time/risk/status filters.
- `GET /transactions/{transaction_id}`: transaction/prediction details where stored.

Response schemas, validation, indexes, and pagination will be implemented together
with the MongoDB repository. No API is implemented in Week 1.
