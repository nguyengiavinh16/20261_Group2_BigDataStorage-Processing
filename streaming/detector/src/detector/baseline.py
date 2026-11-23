"""Redis volume baselines and causal EMA fallback for detector features."""

import json
import math
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime
from time import monotonic

from cryptoguard_common.features import volume_zscore
from cryptoguard_common.timeutil import US_PER_SECOND, UTC


@dataclass(frozen=True)
class BaselineSettings:
    max_age_days: int = 3
    min_windows: int = 2
    cache_seconds: int = 60
    cache_max_entries: int = 512
    socket_timeout_seconds: float = 0.25
    ema_alpha: float = 0.2
    ema_min_windows: int = 5

    def __post_init__(self):
        for name in (
            "max_age_days",
            "min_windows",
            "cache_seconds",
            "cache_max_entries",
            "ema_min_windows",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"detector.baseline.{name} must be a positive integer")
        if not _finite_number(self.socket_timeout_seconds) or self.socket_timeout_seconds <= 0:
            raise ValueError("detector.baseline.socket_timeout_seconds must be positive and finite")
        if not _finite_number(self.ema_alpha) or not 0 < self.ema_alpha < 1:
            raise ValueError("detector.baseline.ema_alpha must be between 0 and 1")


@dataclass(frozen=True)
class RedisBaseline:
    mean: float
    std: float
    n_windows: int
    as_of_date: date


@dataclass(frozen=True)
class BaselineLookup:
    snapshot: RedisBaseline | None
    status: str


@dataclass(frozen=True)
class EmaStats:
    mean: float
    std: float
    n_windows: int


@dataclass(frozen=True)
class EmaWindow:
    start_ts_us: int
    history: EmaStats | None
    sample: float | None


@dataclass(frozen=True)
class ScoredFeature:
    feature: dict
    baseline_source: str | None
    redis_status: str


def _finite_number(value) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def parse_baseline(payload: bytes | str) -> RedisBaseline:
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("Redis baseline must be a JSON object")
    mean = data.get("mean_volume_usd")
    std = data.get("std_volume_usd")
    windows = data.get("n_windows")
    if not _finite_number(mean) or mean <= 0 or not _finite_number(std) or std < 0:
        raise ValueError(
            "Redis baseline requires a positive mean and non-negative standard deviation"
        )
    if type(windows) is not int or windows < 0:
        raise ValueError("Redis baseline requires a non-negative integer n_windows")
    return RedisBaseline(float(mean), float(std), windows, date.fromisoformat(data["as_of_date"]))


class RedisBaselineReader:
    """Bound Redis lookups per worker and validate freshness against event time."""

    def __init__(
        self,
        get: Callable[[str], bytes | str | None],
        settings: BaselineSettings,
        *,
        redis_errors: tuple[type[Exception], ...],
        clock: Callable[[], float] = monotonic,
    ):
        self.get = get
        self.settings = settings
        self.redis_errors = redis_errors
        self.clock = clock
        self.cache: OrderedDict[tuple[str | None, str], tuple[float, BaselineLookup]] = (
            OrderedDict()
        )

    def lookup(self, feature: dict) -> BaselineLookup:
        dt = datetime.fromtimestamp(feature["window_start_ts_us"] // US_PER_SECOND, tz=UTC)
        key = f"baseline:{feature['symbol']}:{feature['exchange']}:{dt.hour}"
        run_id = feature.get("replay_run_id")
        cache_key = run_id, key
        now = self.clock()
        cached = self.cache.pop(cache_key, None)
        if cached is not None and cached[0] > now:
            self.cache[cache_key] = cached
            lookup = cached[1]
        else:
            try:
                payload = self.get(key)
            except self.redis_errors:
                return BaselineLookup(None, "redis_error")

            lookup = BaselineLookup(None, "missing")
            if payload is not None:
                try:
                    lookup = BaselineLookup(parse_baseline(payload), "hit")
                except (KeyError, TypeError, ValueError, RecursionError):
                    lookup = BaselineLookup(None, "invalid")
            self.cache[cache_key] = (now + self.settings.cache_seconds, lookup)
            if len(self.cache) > self.settings.cache_max_entries:
                self.cache.popitem(last=False)

        snapshot = lookup.snapshot
        if snapshot is None:
            return lookup
        age = (dt.date() - snapshot.as_of_date).days
        if age < 0 or (run_id is not None and age == 0):
            return BaselineLookup(None, "future")
        if age > self.settings.max_age_days:
            return BaselineLookup(None, "stale")
        if snapshot.n_windows < self.settings.min_windows:
            return BaselineLookup(None, "insufficient_samples")
        return lookup


def _include_sample(stats: EmaStats | None, sample: float | None, alpha: float) -> EmaStats | None:
    if sample is None:
        return stats
    if stats is None:
        return EmaStats(sample, 0.0, 1)
    delta = sample - stats.mean
    std = math.hypot(math.sqrt(1 - alpha) * stats.std, math.sqrt(alpha * (1 - alpha)) * delta)
    return EmaStats(stats.mean + alpha * delta, std, stats.n_windows + 1)


def score_feature(
    feature: dict,
    lookup: BaselineLookup,
    state: EmaWindow | None,
    settings: BaselineSettings,
) -> tuple[ScoredFeature, EmaWindow]:
    """Score from prior windows. A revision replaces the latest sample without double counting."""
    start = feature["window_start_ts_us"]
    volume = feature.get("volume_usd")
    sample = float(volume) if _finite_number(volume) and volume >= 0 else None
    if state is None:
        history = None
        next_state = EmaWindow(start, history, sample)
    elif start > state.start_ts_us:
        history = _include_sample(state.history, state.sample, settings.ema_alpha)
        next_state = EmaWindow(start, history, sample)
    elif start == state.start_ts_us:
        history = state.history
        next_state = replace(state, sample=sample)
    else:
        # A revision of an older window must not use newer samples or advance the EMA.
        history = None
        next_state = state

    snapshot = lookup.snapshot
    if snapshot is not None:
        mean, std, source = snapshot.mean, snapshot.std, "redis"
    elif history is not None and history.n_windows >= settings.ema_min_windows and history.mean > 0:
        mean, std, source = history.mean, history.std, "ema"
    else:
        mean, std, source = None, None, None

    try:
        score = volume_zscore(sample, mean, std)
    except ZeroDivisionError:
        score = None
    output = feature | {
        "baseline_volume_usd": mean,
        "volume_z": score if _finite_number(score) else None,
    }
    return ScoredFeature(output, source, lookup.status), next_state
