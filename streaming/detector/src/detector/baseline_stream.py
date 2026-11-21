"""PyFlink adapter for Redis lookup and keyed EMA state."""

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

from detector.baseline import BaselineSettings, RedisBaselineReader, score_feature


def _baseline_state_key(feature: dict) -> tuple[str, str, str]:
    return feature.get("replay_run_id") or "", feature["symbol"], feature["exchange"]


class ScoreBaseline(KeyedProcessFunction):
    def __init__(self, redis_url: str, settings: BaselineSettings):
        self.redis_url = redis_url
        self.settings = settings

    def open(self, runtime_context):
        timeout = self.settings.socket_timeout_seconds
        self.client = Redis.from_url(
            self.redis_url,
            socket_connect_timeout=timeout,
            socket_timeout=timeout,
            retry=Retry(NoBackoff(), 0),
        )
        self.reader = RedisBaselineReader(
            self.client.get, self.settings, redis_errors=(RedisError,)
        )
        self.ema = runtime_context.get_state(
            ValueStateDescriptor("detector-volume-ema", Types.PICKLED_BYTE_ARRAY())
        )
        metrics = runtime_context.get_metrics_group()
        self.missing = metrics.counter("cg_detector_baseline_missing_total")
        self.errors = metrics.counter("cg_detector_redis_errors_total")

    def close(self):
        self.client.close()

    def process_element(self, feature, ctx):
        lookup = self.reader.lookup(feature)
        if lookup.status == "redis_error":
            self.errors.inc()
        scored, state = score_feature(feature, lookup, self.ema.value(), self.settings)
        self.ema.update(state)
        if scored.baseline_source is None:
            self.missing.inc()
        yield scored


def enrich_features(features, redis_url: str, settings: BaselineSettings):
    key_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()])
    return features.key_by(_baseline_state_key, key_type=key_type).process(
        ScoreBaseline(redis_url, settings)
    )
