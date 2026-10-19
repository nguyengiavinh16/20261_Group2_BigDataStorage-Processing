// DRAFT W1 - MongoDB collections & indexes per nosql-access-patterns.md
// Run: mongosh "mongodb://localhost:27017/fraud" configs/mongodb/init-indexes.js
// Every index below maps to one access pattern (AP-x). Do not add indexes that no query uses.

const db_ = db.getSiblingDB("fraud");

// ---------- predictions: every scored transaction (1 doc per transaction_id) ----------
db_.createCollection("predictions");
// AP1 lookup by transaction_id -> uses _id = transaction_id (no separate index needed)
// AP5 dashboard: counts/distributions by time and transaction type
db_.predictions.createIndex({ scored_at: -1 }, { name: "ix_scored_at" });
db_.predictions.createIndex({ event_time: -1 }, { name: "ix_event_time" });
db_.predictions.createIndex({ transaction_type: 1, scored_at: -1 }, { name: "ix_type_scored_at" });

// ---------- alerts: only transactions with alert_triggered = true, with a status lifecycle ----------
db_.createCollection("alerts", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "event_time", "scored_at", "fraud_score", "risk_level", "status", "model_version"],
      properties: {
        risk_level: { enum: ["LOW", "MEDIUM", "HIGH"] },
        status: { enum: ["NEW", "ACKNOWLEDGED", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "CLOSED"] },
        fraud_score: { bsonType: ["double", "int"], minimum: 0, maximum: 1 }
      }
    }
  }
});
// AP2 latest alerts within a time range
db_.alerts.createIndex({ scored_at: -1 }, { name: "ix_scored_at" });
// AP3 filter by risk_level + time (ESR: equality first, then sort/range)
db_.alerts.createIndex({ risk_level: 1, scored_at: -1 }, { name: "ix_risk_scored_at" });
// AP4 alert queue by status (NEW/ACKNOWLEDGED) + time
db_.alerts.createIndex({ status: 1, scored_at: -1 }, { name: "ix_status_scored_at" });
// AP6 investigation by source account
db_.alerts.createIndex({ source_account: 1, event_time: -1 }, { name: "ix_account_event_time" });

// ---------- stream_metrics (optional): valid/invalid/alert event counts per 1-minute window ----------
// _id = "<window_start ISO>" ; written with upsert $set (NOT $inc) so retries do not double-count
db_.createCollection("stream_metrics");
db_.stream_metrics.createIndex({ window_start: -1 }, { name: "ix_window_start" });

print("Collections:", db_.getCollectionNames());
