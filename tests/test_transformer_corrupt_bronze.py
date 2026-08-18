"""Regression tests for replaying Bronze files that did not land whole.

Bronze is the source of truth every other layer replays from, so a Bronze file
that was truncated by an unclean shutdown is the one artifact that cannot be
recovered from anywhere else. What it still holds must be salvaged, and one bad
file must not take the rest of the day's replay down with it.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from heber.writer.transformer import BronzeToSilverTransformer

pytestmark = pytest.mark.unit


def _event(i: int) -> dict:
    ts = datetime(2026, 6, 18, 15, 45, tzinfo=UTC).isoformat()
    return {
        "event_id": f"evt-{i}",
        "provider": "alpaca",
        "feed": "darkpool",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": ts,
        "ts_ingest": ts,
        "ts_available": ts,
        "source": "test",
        "schema_version": "1.0.0",
        "payload": {"underlying": "AAPL", "price": 1.0 + i, "size": 100 + i},
    }


def _write_bronze(path: Path, events: list[dict], truncate_to: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")
    if truncate_to is not None:
        complete = path.read_bytes()
        path.write_bytes(complete[:truncate_to])


def test_a_truncated_bronze_file_still_yields_what_it_holds(tmp_path: Path) -> None:
    """A file cut short mid-stream must give up its readable prefix, not nothing.

    gzip raises EOFError part-way through iteration, so a reader that lets it
    escape discards every record it had already decoded.
    """
    bronze = tmp_path / "bronze"
    src = bronze / "provider=alpaca" / "feed=darkpool" / "dt=2026-06-18" / "hour=15" / "events-1.jsonl.gz"
    _write_bronze(src, [_event(i) for i in range(500)])
    full_size = src.stat().st_size
    _write_bronze(src, [_event(i) for i in range(500)], truncate_to=full_size // 2)

    transformer = BronzeToSilverTransformer(bronze_path=bronze, silver_path=tmp_path / "silver")
    rows = transformer._read_bronze_file(src, "darkpool")

    assert rows, "a truncated file yielded nothing; its readable prefix was discarded"
    assert len(rows) < 500, "expected a partial read from a truncated file"


def test_one_corrupt_bronze_file_does_not_abort_the_whole_date(tmp_path: Path) -> None:
    """The other files for that date must still be replayed.

    Bronze holds ~1,800 files for a busy day. Letting one bad file abort the run
    makes the whole date un-replayable, which is exactly when replay matters.
    """
    bronze = tmp_path / "bronze"
    dt_dir = bronze / "provider=alpaca" / "feed=darkpool" / "dt=2026-06-18" / "hour=15"

    good_a = dt_dir / "events-a.jsonl.gz"
    bad = dt_dir / "events-b.jsonl.gz"
    good_b = dt_dir / "events-c.jsonl.gz"

    _write_bronze(good_a, [_event(i) for i in range(10)])
    _write_bronze(good_b, [_event(i) for i in range(100, 110)])
    _write_bronze(bad, [_event(i) for i in range(200, 700)])
    bad.write_bytes(bad.read_bytes()[: bad.stat().st_size // 2])

    transformer = BronzeToSilverTransformer(bronze_path=bronze, silver_path=tmp_path / "silver")
    written = transformer.transform("darkpool", dt="2026-06-18", provider="alpaca")

    assert written >= 20, f"good files were not replayed past the corrupt one (wrote {written})"


def test_a_body_that_inflates_to_non_utf8_is_skipped(tmp_path: Path) -> None:
    """Bytes that inflate cleanly but are not UTF-8 raise from the text wrapper.

    UnicodeDecodeError is neither an OSError nor a zlib error, so it escaped too.
    Built by hand rather than by flipping bits, so it does not depend on which
    zlib build happens to be installed.
    """
    bronze = tmp_path / "bronze"
    src = bronze / "provider=alpaca" / "feed=darkpool" / "dt=2026-06-18" / "hour=15" / "events-1.jsonl.gz"
    src.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(src, "wb") as fh:
        fh.write(b'{"a": "\xff\xfe"}\n' * 10)

    transformer = BronzeToSilverTransformer(bronze_path=bronze, silver_path=tmp_path / "silver")
    rows = transformer._read_bronze_file(src, "darkpool")  # must not raise

    assert rows == []


def test_a_crc_failure_discards_its_salvage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Content that inflated but failed its checksum is unverified, not salvage.

    A flipped digit still parses as valid JSON, so returning those rows would
    launder corrupt data into Silver as though it were recovered.
    """
    bronze = tmp_path / "bronze"
    src = bronze / "provider=alpaca" / "feed=darkpool" / "dt=2026-06-18" / "hour=15" / "events-1.jsonl.gz"
    _write_bronze(src, [_event(i) for i in range(20)])

    real_open = gzip.open

    def crc_failing_open(*args: object, **kwargs: object):
        handle = real_open(*args, **kwargs)  # type: ignore[arg-type]
        lines = list(handle)

        def gen():
            yield from lines
            raise gzip.BadGzipFile("CRC check failed 0x1 != 0x2")

        return _CtxIter(gen())

    monkeypatch.setattr(gzip, "open", crc_failing_open)
    transformer = BronzeToSilverTransformer(bronze_path=bronze, silver_path=tmp_path / "silver")
    rows = transformer._read_bronze_file(src, "darkpool")

    assert rows == [], "rows from a CRC-failed file were passed off as salvage"


def test_a_dying_volume_does_not_report_a_successful_replay(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If most files cannot be read, the replay must fail loudly.

    A dropped mount raises the same error on every file. Logging each one and
    returning a record count is how a total outage reports success and exits 0.
    """
    bronze = tmp_path / "bronze"
    dt_dir = bronze / "provider=alpaca" / "feed=darkpool" / "dt=2026-06-18" / "hour=15"
    for name in ("a", "b", "c", "d"):
        _write_bronze(dt_dir / f"events-{name}.jsonl.gz", [_event(i) for i in range(5)])

    def exploding_open(*args: object, **kwargs: object):
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(gzip, "open", exploding_open)
    transformer = BronzeToSilverTransformer(bronze_path=bronze, silver_path=tmp_path / "silver")

    with pytest.raises(OSError, match="refusing to report a partial replay as complete"):
        transformer.transform("darkpool", dt="2026-06-18", provider="alpaca")


def test_appledouble_sidecars_are_not_read_as_bronze(tmp_path: Path) -> None:
    """`._name` sidecars litter this volume and match the replay glob.

    Reporting each as a damaged Bronze file buries the real ones and would skew
    any failure budget computed from the count.
    """
    bronze = tmp_path / "bronze"
    dt_dir = bronze / "provider=alpaca" / "feed=darkpool" / "dt=2026-06-18" / "hour=15"
    _write_bronze(dt_dir / "events-a.jsonl.gz", [_event(i) for i in range(5)])
    (dt_dir / "._events-a.jsonl.gz").write_bytes(b"AppleDouble garbage")

    transformer = BronzeToSilverTransformer(bronze_path=bronze, silver_path=tmp_path / "silver")
    written = transformer.transform("darkpool", dt="2026-06-18", provider="alpaca")

    assert written == 5
    assert transformer._read_failures == [], f"sidecar counted as a failure: {transformer._read_failures}"


class _CtxIter:
    """Minimal context-manager wrapper around an iterator, for gzip.open fakes."""

    def __init__(self, it):
        self._it = it

    def __enter__(self):
        return self._it

    def __exit__(self, *exc: object) -> bool:
        return False

    def __iter__(self):
        return self._it
