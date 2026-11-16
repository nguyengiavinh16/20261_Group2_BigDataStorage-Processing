"""Reference (single-machine) taint propagation: the SPEC for the Spark/GraphFrames job (BAT-09).

The Spark implementation must produce the same numbers as this function on the unit-test graphs.
Semantics ("haircut" taint):
    taint(seed) = 1.0 (seeds = labelled bad wallets valid at as_of time)
    taint(v)    = min(1, decay * Σ_u taint(u) * usd(u→v) / total_in_usd(v))      for non-seed v
Exchange wallets receive taint but do NOT propagate it (they pool millions of users).
Iterate `max_hops` rounds (each round uses the previous round's values, Pregel-style).
hops_to_nearest_bad = BFS distance from any seed along edges (ignoring exchange wallets as intermediates).
"""

from collections import defaultdict, deque


def propagate_taint(
    edges: list[tuple[str, str, float]],
    seeds: set[str],
    *,
    decay: float = 0.5,
    max_hops: int = 3,
    exchange_wallets: frozenset[str] = frozenset(),
) -> dict[str, float]:
    """edges: (src, dst, usd). Returns taint for every node with taint > 0."""
    incoming: dict[str, list[tuple[str, float]]] = defaultdict(list)
    total_in: dict[str, float] = defaultdict(float)
    for src, dst, usd in edges:
        if usd <= 0 or src == dst:
            continue
        incoming[dst].append((src, usd))
        total_in[dst] += usd

    taint = {s: 1.0 for s in seeds}
    for _ in range(max_hops):
        nxt = dict(taint)
        for v, ins in incoming.items():
            if v in seeds:
                continue
            acc = sum(taint.get(u, 0.0) * usd for u, usd in ins if u not in exchange_wallets)
            if acc > 0:
                nxt[v] = min(1.0, decay * acc / total_in[v])
        taint = nxt
    return {k: v for k, v in taint.items() if v > 0}


def hops_from_seeds(
    edges: list[tuple[str, str, float]],
    seeds: set[str],
    *,
    max_hops: int = 3,
    exchange_wallets: frozenset[str] = frozenset(),
) -> dict[str, int]:
    out: dict[str, list[str]] = defaultdict(list)
    for src, dst, usd in edges:
        if usd > 0 and src != dst:
            out[src].append(dst)
    dist = {s: 0 for s in seeds}
    queue = deque(seeds)
    while queue:
        u = queue.popleft()
        if dist[u] >= max_hops or (u in exchange_wallets and u not in seeds):
            continue
        for v in out[u]:
            if v not in dist:
                dist[v] = dist[u] + 1
                queue.append(v)
    return dist
