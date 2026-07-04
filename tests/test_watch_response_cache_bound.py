"""The watch feature extractor's response cache must stay bounded.

Eviction used to happen only lazily on a cache hit after TTL expiry, so a
long-running watch service accumulated one entry per distinct
endpoint|symbol|params key forever (audit F-8).
"""

from __future__ import annotations

from heber.watch.features import RESPONSE_CACHE_MAX_ENTRIES, AlertFeatureExtractor


def _extractor(**kwargs) -> AlertFeatureExtractor:
    return AlertFeatureExtractor(redis=None, gateway_url="http://gateway.test", **kwargs)


def test_cache_evicts_oldest_beyond_max_entries() -> None:
    extractor = _extractor(response_cache_max_entries=3)

    for i in range(5):
        extractor._cache_set(f"key-{i}", {"i": i})

    assert len(extractor._response_cache) == 3
    assert extractor._cache_get("key-0") is None
    assert extractor._cache_get("key-1") is None
    assert extractor._cache_get("key-4") == {"i": 4}


def test_cache_get_refreshes_lru_order() -> None:
    extractor = _extractor(response_cache_max_entries=2)

    extractor._cache_set("a", {"v": 1})
    extractor._cache_set("b", {"v": 2})
    assert extractor._cache_get("a") == {"v": 1}  # refresh "a"
    extractor._cache_set("c", {"v": 3})  # evicts "b", not "a"

    assert extractor._cache_get("a") == {"v": 1}
    assert extractor._cache_get("b") is None


def test_overwriting_existing_key_does_not_evict() -> None:
    extractor = _extractor(response_cache_max_entries=2)

    extractor._cache_set("a", {"v": 1})
    extractor._cache_set("b", {"v": 2})
    extractor._cache_set("a", {"v": 99})

    assert len(extractor._response_cache) == 2
    assert extractor._cache_get("a") == {"v": 99}
    assert extractor._cache_get("b") == {"v": 2}


def test_default_bound_is_sane() -> None:
    extractor = _extractor()
    assert extractor.response_cache_max_entries == RESPONSE_CACHE_MAX_ENTRIES
    assert 100 <= RESPONSE_CACHE_MAX_ENTRIES <= 100_000
