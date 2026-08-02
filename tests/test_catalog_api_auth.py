"""Catalog API bearer-token auth (config-gated by HEBER_CATALOG_AUTH_ENABLED).

Behavior contract:
- Auth is OFF by default; with auth disabled every route behaves exactly as before.
- With auth enabled, every route except /health and the docs endpoints requires a
  bearer token validated against the persisted token records
  (heber.catalog.access_control hashes; bootstrap via `heber catalog-token create`).
- Missing/invalid credentials -> 401 UNAUTHORIZED (error envelope).
- Valid token without the required scope on a mutating route -> 403 FORBIDDEN.
- Token store missing/unreadable/corrupt -> 503 on protected routes and on
  /health, and the process stays up so the file can be repaired.
"""

from __future__ import annotations

import json
import sys
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from heber.catalog import auth as catalog_auth
from heber.catalog.api import app
from heber.config import Settings, settings

pytestmark = pytest.mark.unit


BACKFILL_BODY = {
    "provider": "unusualwhales",
    "feed": "flow_alerts",
    "instrument_keys": ["equity:AAPL"],
    "start_date": "2026-01-01",
    "end_date": "2026-01-02",
}

# Routes that must stay reachable without credentials even when auth is enabled.
OPEN_PATHS = {
    "/health",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
    "/openapi.json",
}

# (method, path) pairs that mutate catalog state and therefore need write scope.
WRITE_ROUTES = {
    ("POST", "/datasets"),
    ("POST", "/api/v1/datasets"),
    ("PUT", "/api/v1/instruments/{key}"),
    ("POST", "/api/v1/backfill"),
}


@pytest.fixture(autouse=True)
def _reset_auth_state():
    catalog_auth.reset_access_control()
    yield
    catalog_auth.reset_access_control()


@pytest.fixture
def client() -> TestClient:
    # No context manager: lifespan (DB bootstrap) must not run for these tests.
    return TestClient(app)


@pytest.fixture
def tokens_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "tokens.json"
    monkeypatch.setattr(settings, "catalog_auth_tokens_file", path)
    return path


@pytest.fixture
def auth_enabled(tokens_file: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(settings, "catalog_auth_enabled", True)
    return tokens_file


def _mint(tokens_file: Path, scopes: list[str]) -> str:
    raw, _token = catalog_auth.create_token_record(
        tokens_file, project_id="test-project", name="test-token", scopes=scopes
    )
    catalog_auth.reset_access_control()
    return raw


def _bearer(raw: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {raw}"}


class TestAuthDisabled:
    def test_settings_default_is_disabled(self) -> None:
        assert Settings.model_fields["catalog_auth_enabled"].default is False

    def test_read_route_open_without_token(self, client: TestClient) -> None:
        resp = client.get("/api/v1/backfill")
        assert resp.status_code == 200

    def test_write_route_open_without_token(self, client: TestClient) -> None:
        resp = client.post("/api/v1/backfill", json=BACKFILL_BODY)
        assert resp.status_code == 201


class TestTokensPathResolution:
    def test_default_path_lives_under_data_root(self) -> None:
        cfg = Settings(_env_file=None, data_root=Path("/tmp/heber-data"))
        assert cfg.catalog_auth_tokens_path == Path("/tmp/heber-data/_catalog_auth/tokens.json")

    def test_explicit_path_wins(self) -> None:
        cfg = Settings(_env_file=None, data_root=Path("/tmp/heber-data"), catalog_auth_tokens_file=Path("/etc/t.json"))
        assert cfg.catalog_auth_tokens_path == Path("/etc/t.json")

    def test_blank_env_value_falls_back_to_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # `HEBER_CATALOG_AUTH_TOKENS_FILE=` in a .env parses as Path("."), which
        # would silently point auth at the working directory.
        monkeypatch.setenv("HEBER_CATALOG_AUTH_TOKENS_FILE", "")
        cfg = Settings(_env_file=None, data_root=Path("/tmp/heber-data"))
        assert cfg.catalog_auth_tokens_path == Path("/tmp/heber-data/_catalog_auth/tokens.json")


class TestAuthEnabled:
    def test_missing_token_401_with_envelope(self, client: TestClient, auth_enabled: Path) -> None:
        _mint(auth_enabled, ["read"])
        resp = client.get("/api/v1/backfill")
        assert resp.status_code == 401
        assert resp.json()["error"]["code"] == "UNAUTHORIZED"
        assert resp.headers.get("WWW-Authenticate") == "Bearer"

    def test_invalid_token_401(self, client: TestClient, auth_enabled: Path) -> None:
        _mint(auth_enabled, ["read"])
        resp = client.get("/api/v1/backfill", headers=_bearer("not-a-real-token"))
        assert resp.status_code == 401
        assert resp.json()["error"]["code"] == "UNAUTHORIZED"

    def test_valid_read_token_200(self, client: TestClient, auth_enabled: Path) -> None:
        raw = _mint(auth_enabled, ["read"])
        resp = client.get("/api/v1/backfill", headers=_bearer(raw))
        assert resp.status_code == 200

    def test_read_token_cannot_write_403(self, client: TestClient, auth_enabled: Path) -> None:
        raw = _mint(auth_enabled, ["read"])
        resp = client.post("/api/v1/backfill", json=BACKFILL_BODY, headers=_bearer(raw))
        assert resp.status_code == 403
        assert resp.json()["error"]["code"] == "FORBIDDEN"

    def test_write_token_can_write_201(self, client: TestClient, auth_enabled: Path) -> None:
        raw = _mint(auth_enabled, ["read", "write"])
        resp = client.post("/api/v1/backfill", json=BACKFILL_BODY, headers=_bearer(raw))
        assert resp.status_code == 201

    def test_write_scope_alone_also_grants_read(self, client: TestClient, auth_enabled: Path) -> None:
        raw = _mint(auth_enabled, ["write"])
        assert client.get("/api/v1/backfill", headers=_bearer(raw)).status_code == 200

    def test_admin_scope_grants_read_and_write(self, client: TestClient, auth_enabled: Path) -> None:
        # `admin` is offered by the CLI; a token that carries it must not be
        # rejected everywhere, which would read as a lockout to the operator.
        raw = _mint(auth_enabled, ["admin"])
        assert client.get("/api/v1/backfill", headers=_bearer(raw)).status_code == 200
        assert client.post("/api/v1/backfill", json=BACKFILL_BODY, headers=_bearer(raw)).status_code == 201

    def test_wildcard_token_can_write_201(self, client: TestClient, auth_enabled: Path) -> None:
        raw = _mint(auth_enabled, ["*"])
        resp = client.post("/api/v1/backfill", json=BACKFILL_BODY, headers=_bearer(raw))
        assert resp.status_code == 201

    def test_revoked_token_401(self, client: TestClient, auth_enabled: Path) -> None:
        raw = _mint(auth_enabled, ["read"])
        records = json.loads(auth_enabled.read_text())
        records[0]["revoked"] = True
        auth_enabled.write_text(json.dumps(records))
        catalog_auth.reset_access_control()
        resp = client.get("/api/v1/backfill", headers=_bearer(raw))
        assert resp.status_code == 401

    def test_expired_token_401(self, client: TestClient, auth_enabled: Path) -> None:
        raw = _mint(auth_enabled, ["read"])
        records = json.loads(auth_enabled.read_text())
        records[0]["expires_at"] = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        auth_enabled.write_text(json.dumps(records))
        catalog_auth.reset_access_control()
        resp = client.get("/api/v1/backfill", headers=_bearer(raw))
        assert resp.status_code == 401

    def test_health_stays_open(self, client: TestClient, auth_enabled: Path) -> None:
        # Docker healthcheck depends on /health working with zero credentials.
        # Without a reachable Postgres this returns 503 — auth must never be the
        # reason it fails.
        _mint(auth_enabled, ["read"])
        resp = client.get("/health")
        assert resp.status_code not in (401, 403)

    def test_docs_and_openapi_stay_open(self, client: TestClient, auth_enabled: Path) -> None:
        _mint(auth_enabled, ["read"])
        assert client.get("/openapi.json").status_code == 200
        assert client.get("/docs").status_code == 200


class TestRouteCoverage:
    """Every registered route must carry the right auth dependency."""

    def test_all_routes_are_covered(self) -> None:
        for route in app.routes:
            if not isinstance(route, APIRoute):
                continue
            if route.path in OPEN_PATHS:
                continue
            dep_calls = {dep.call for dep in route.dependant.dependencies}
            expected_write = any((method, route.path) in WRITE_ROUTES for method in route.methods or set())
            if expected_write:
                assert catalog_auth.require_write in dep_calls, f"{route.methods} {route.path} missing require_write"
            else:
                assert catalog_auth.require_read in dep_calls, f"{route.methods} {route.path} missing require_read"

    def test_health_route_has_no_auth_dependency(self) -> None:
        for route in app.routes:
            if isinstance(route, APIRoute) and route.path == "/health":
                dep_calls = {dep.call for dep in route.dependant.dependencies}
                assert catalog_auth.require_read not in dep_calls
                assert catalog_auth.require_write not in dep_calls


class TestTokenBootstrap:
    def test_missing_tokens_file_fails_loud(self, auth_enabled: Path) -> None:
        # File never created: the lazy load must fail with a message pointing at
        # the bootstrap CLI, not silently 401 forever.
        with pytest.raises(catalog_auth.TokenStoreUnavailable, match="catalog-token create"):
            catalog_auth.get_access_control()

    def test_token_file_is_readable_by_the_container_user(self, tmp_path: Path) -> None:
        # The catalog runs in Docker as uid 10000 on the same bind mount this CLI
        # writes to as the host user. An owner-only file would take the whole
        # catalog down; the file holds hashes only, never a usable credential.
        path = tmp_path / "sub" / "tokens.json"
        catalog_auth.create_token_record(path, project_id="p", name="n", scopes=["read"])
        assert path.stat().st_mode & 0o004, "token file must be readable by other users"
        assert path.parent.stat().st_mode & 0o001, "token directory must be traversable by other users"

    def test_create_token_record_roundtrip(self, tmp_path: Path) -> None:
        path = tmp_path / "tokens.json"
        raw, token = catalog_auth.create_token_record(
            path, project_id="kairos", name="ci-token", scopes=["read", "write"]
        )
        assert path.exists()
        # Only the SHA-256 hash may be persisted — never the raw token.
        assert raw not in path.read_text()

        manager = catalog_auth.load_access_control(path)
        validated = manager.validate_token(raw)
        assert validated is not None
        assert validated.token_id == token.token_id
        assert validated.project_id == "kairos"
        assert validated.scopes == ["read", "write"]

    def test_create_token_record_appends(self, tmp_path: Path) -> None:
        path = tmp_path / "tokens.json"
        raw_a, _ = catalog_auth.create_token_record(path, project_id="a", name="one", scopes=["read"])
        raw_b, _ = catalog_auth.create_token_record(path, project_id="b", name="two", scopes=["write"])
        manager = catalog_auth.load_access_control(path)
        assert manager.validate_token(raw_a) is not None
        assert manager.validate_token(raw_b) is not None

    def test_cli_create_prints_raw_token_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from heber.cli import main

        path = tmp_path / "tokens.json"
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "heber",
                "catalog-token",
                "create",
                "--project",
                "kairos",
                "--name",
                "bootstrap",
                "--scopes",
                "read,write",
                "--tokens-file",
                str(path),
            ],
        )
        assert main() == 0
        out = capsys.readouterr().out
        manager = catalog_auth.load_access_control(path)
        # The printed output must contain the raw token (last non-empty line).
        raw = [line for line in out.splitlines() if line.strip()][-1].strip()
        assert manager.validate_token(raw) is not None

    def test_cli_create_requires_project_and_name(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from heber.cli import main

        monkeypatch.setattr(
            sys,
            "argv",
            ["heber", "catalog-token", "create", "--tokens-file", str(tmp_path / "t.json")],
        )
        assert main() != 0

    def test_cli_rejects_unknown_scope(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from heber.cli import main

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "heber",
                "catalog-token",
                "create",
                "--project",
                "p",
                "--name",
                "n",
                "--scopes",
                "read,bogus",
                "--tokens-file",
                str(tmp_path / "t.json"),
            ],
        )
        assert main() != 0
        assert not (tmp_path / "t.json").exists()

    def test_cli_rejects_nonpositive_expiry(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from heber.cli import main

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "heber",
                "catalog-token",
                "create",
                "--project",
                "p",
                "--name",
                "n",
                "--expires-days",
                "0",
                "--tokens-file",
                str(tmp_path / "t.json"),
            ],
        )
        assert main() != 0
        assert not (tmp_path / "t.json").exists()

    def test_corrupt_tokens_file_fails_loud_with_path(self, tmp_path: Path) -> None:
        path = tmp_path / "tokens.json"
        path.write_text("{this is not json")
        with pytest.raises(catalog_auth.TokenStoreUnavailable, match="tokens.json"):
            catalog_auth.load_access_control(path)

    @pytest.mark.parametrize(
        "records",
        [
            pytest.param([{"token_id": "tok_1"}], id="missing_fields"),
            pytest.param([None], id="null_record"),
            pytest.param(["not-a-record"], id="string_record"),
            pytest.param([123], id="number_record"),
            pytest.param([{"token_id": 1, "project_id": "p", "token_hash": "h", "name": "n"}], id="nonstring_id"),
            pytest.param(
                [{"token_id": "t", "project_id": "p", "token_hash": "h", "name": "n", "scopes": "read"}],
                id="nonlist_scopes",
            ),
            pytest.param(
                [{"token_id": "t", "project_id": "p", "token_hash": "h", "name": "n", "expires_at": 12345}],
                id="nonstring_expiry",
            ),
        ],
    )
    def test_malformed_record_fails_loud(self, tmp_path: Path, records: list[object]) -> None:
        # Hand-edited junk must surface as TokenStoreUnavailable, never as a raw
        # TypeError/AttributeError that would escape into a 500 or kill startup.
        path = tmp_path / "tokens.json"
        path.write_text(json.dumps(records))
        with pytest.raises(catalog_auth.TokenStoreUnavailable, match="malformed record"):
            catalog_auth.load_access_control(path)

    @pytest.mark.parametrize(
        "raw_bytes",
        [
            pytest.param(b"[\xff]", id="invalid_utf8"),
            pytest.param(b"[" * 20000 + b"]" * 20000, id="recursion_depth"),
            pytest.param(b"", id="empty_file"),
        ],
    )
    def test_unparseable_bytes_fail_as_token_store_unavailable(self, tmp_path: Path, raw_bytes: bytes) -> None:
        # Anything that is not valid JSON records must become a controlled
        # failure — never a UnicodeDecodeError or RecursionError escaping into
        # a 500 or killing the process.
        path = tmp_path / "tokens.json"
        path.write_bytes(raw_bytes)
        with pytest.raises(catalog_auth.TokenStoreUnavailable):
            catalog_auth.load_access_control(path)

    def test_create_does_not_relax_an_existing_directory(self, tmp_path: Path) -> None:
        # Minting a token must not widen a directory the operator locked down.
        parent = tmp_path / "locked"
        parent.mkdir(mode=0o700)
        try:
            catalog_auth.create_token_record(parent / "tokens.json", project_id="p", name="n", scopes=["read"])
            assert parent.stat().st_mode & 0o777 == 0o700
        finally:
            parent.chmod(0o755)

    def test_locked_down_directory_is_reported_not_silently_broken(self, tmp_path: Path) -> None:
        # Host mint succeeds but uid 10000 cannot traverse the directory, so the
        # catalog would sit on 503 with no explanation. Say it at mint time.
        parent = tmp_path / "locked"
        parent.mkdir(mode=0o700)
        try:
            path = parent / "tokens.json"
            catalog_auth.create_token_record(path, project_id="p", name="n", scopes=["read"])
            warning = catalog_auth.unreachable_by_container(path)
            assert warning is not None
            assert "not traversable" in warning
            assert "uid 10000" in warning
        finally:
            parent.chmod(0o755)

    def test_normal_bootstrap_reports_no_warning(self, tmp_path: Path) -> None:
        path = tmp_path / "_catalog_auth" / "tokens.json"
        catalog_auth.create_token_record(path, project_id="p", name="n", scopes=["read"])
        assert catalog_auth.unreachable_by_container(path) is None

    def test_restrictive_data_root_is_reported(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # data_root maps into the container, so a locked-down mount root blocks
        # the container even when the token file's own directory is fine.
        data_root = tmp_path / "data"
        data_root.mkdir(mode=0o755)
        monkeypatch.setattr(settings, "data_root", data_root)
        path = data_root / "_catalog_auth" / "tokens.json"
        catalog_auth.create_token_record(path, project_id="p", name="n", scopes=["read"])
        assert catalog_auth.unreachable_by_container(path) is None

        data_root.chmod(0o700)
        try:
            warning = catalog_auth.unreachable_by_container(path)
            assert warning is not None
            assert str(data_root) in warning
        finally:
            data_root.chmod(0o755)

    def test_group_root_access_is_not_a_false_warning(self) -> None:
        # The container is gid 0; a gid-0-owned 0750/0640 pair is reachable.
        # chown to gid 0 needs privileges, so the bit logic is checked directly.
        import os as _os

        def _stat(mode: int, gid: int) -> _os.stat_result:
            return _os.stat_result((mode, 0, 0, 1, 0, gid, 0, 0, 0, 0))

        assert catalog_auth._reachable(_stat(0o750, 0), 0o001)
        assert catalog_auth._reachable(_stat(0o640, 0), 0o004)
        assert not catalog_auth._reachable(_stat(0o750, 20), 0o001)
        assert not catalog_auth._reachable(_stat(0o640, 20), 0o004)

    def test_unreadable_tokens_file_fails_with_guidance(self, tmp_path: Path) -> None:
        path = tmp_path / "tokens.json"
        catalog_auth.create_token_record(path, project_id="p", name="n", scopes=["read"])
        path.chmod(0o000)
        try:
            with pytest.raises(catalog_auth.TokenStoreUnavailable, match="cannot read"):
                catalog_auth.load_access_control(path)
        finally:
            path.chmod(0o644)


class TestTokenFileReload:
    """Token file edits must apply without a service restart.

    A revoked credential staying live until the next container restart is a
    security hole; the manager reloads when the file changes on disk.
    """

    def test_revocation_applies_without_restart(self, client: TestClient, auth_enabled: Path) -> None:
        raw = _mint(auth_enabled, ["read"])
        assert client.get("/api/v1/backfill", headers=_bearer(raw)).status_code == 200

        records = json.loads(auth_enabled.read_text())
        records[0]["revoked"] = True
        auth_enabled.write_text(json.dumps(records))
        # No cache reset: the running API must pick up the change by itself.
        assert client.get("/api/v1/backfill", headers=_bearer(raw)).status_code == 401

    def test_same_size_same_mtime_edit_still_invalidates(self, client: TestClient, auth_enabled: Path) -> None:
        # Revoking by hand-editing `false` -> `true ` keeps the byte count, and
        # mtime can be restored. The cache must not keep serving the old token.
        import os

        raw = _mint(auth_enabled, ["read"])
        assert client.get("/api/v1/backfill", headers=_bearer(raw)).status_code == 200

        before = auth_enabled.stat()
        text = auth_enabled.read_text()
        assert '"revoked": false' in text
        auth_enabled.write_text(text.replace('"revoked": false', '"revoked": true '))
        assert auth_enabled.stat().st_size == before.st_size
        os.utime(auth_enabled, ns=(before.st_atime_ns, before.st_mtime_ns))

        assert client.get("/api/v1/backfill", headers=_bearer(raw)).status_code == 401

    def test_new_token_recognized_without_restart(self, client: TestClient, auth_enabled: Path) -> None:
        raw_a = _mint(auth_enabled, ["read"])
        assert client.get("/api/v1/backfill", headers=_bearer(raw_a)).status_code == 200

        raw_b, _ = catalog_auth.create_token_record(
            auth_enabled, project_id="second", name="late-join", scopes=["read"]
        )
        # No cache reset: the new token must work immediately.
        assert client.get("/api/v1/backfill", headers=_bearer(raw_b)).status_code == 200


def _fake_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the catalog DB session so /health's SELECT 1 succeeds without Postgres."""
    from contextlib import asynccontextmanager

    class _Session:
        async def execute(self, *args: object, **kwargs: object) -> None:
            return None

    @asynccontextmanager
    async def _factory() -> AsyncIterator[_Session]:
        yield _Session()

    monkeypatch.setattr("heber.catalog.api.async_session", _factory)


class TestHealthAuthVisibility:
    """Silent lockout (all tokens expired/revoked) must be visible on /health."""

    def test_health_reports_zero_valid_tokens(
        self, client: TestClient, auth_enabled: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mint(auth_enabled, ["read"])
        records = json.loads(auth_enabled.read_text())
        records[0]["revoked"] = True
        auth_enabled.write_text(json.dumps(records))
        _fake_db(monkeypatch)

        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["auth"] == {"enabled": True, "valid_tokens": 0}

    def test_health_shape_unchanged_when_auth_disabled(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _fake_db(monkeypatch)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "healthy", "service": "heber-catalog"}


class TestUnusableTokenStore:
    """Auth on + unreadable token store: fail closed, loudly, without exiting.

    A container that crash-loops on a bad file mode cannot be inspected or
    repaired, and one that reports healthy while every route fails is the exact
    blind spot /health exists to close.
    """

    def test_protected_route_returns_503_not_500(self, client: TestClient, auth_enabled: Path) -> None:
        # auth_enabled points at a tokens file that was never created.
        resp = client.get("/api/v1/backfill", headers=_bearer("anything"))
        assert resp.status_code == 503
        assert resp.json()["error"]["code"] == "UNKNOWN_ERROR"
        assert "token store is unavailable" in resp.json()["error"]["message"]

    def test_unauthenticated_request_also_reports_the_outage(self, client: TestClient, auth_enabled: Path) -> None:
        # A 401 here would disguise a server outage as a client mistake.
        resp = client.get("/api/v1/backfill")
        assert resp.status_code == 503

    def test_503_body_does_not_leak_the_token_file_path(self, client: TestClient, auth_enabled: Path) -> None:
        # This response reaches unauthenticated callers; the path belongs in the log.
        resp = client.get("/api/v1/backfill")
        assert str(auth_enabled) not in resp.text
        assert "tokens.json" not in resp.text

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(json.dumps([None]).encode(), id="null_record"),
            pytest.param(b"[\xff]", id="invalid_utf8"),
            pytest.param(b"[" * 20000 + b"]" * 20000, id="recursion_depth"),
            pytest.param(b"not json at all", id="not_json"),
        ],
    )
    def test_corrupt_store_returns_503_not_500(self, client: TestClient, auth_enabled: Path, content: bytes) -> None:
        auth_enabled.write_bytes(content)
        resp = client.get("/api/v1/backfill", headers=_bearer("anything"))
        assert resp.status_code == 503

    def test_corrupt_store_health_returns_503_not_500(
        self, client: TestClient, auth_enabled: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _fake_db(monkeypatch)
        auth_enabled.write_bytes(b"[\xff]")
        resp = client.get("/health")
        assert resp.status_code == 503

    def test_health_503_does_not_leak_the_token_file_path(
        self, client: TestClient, auth_enabled: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # /health is unauthenticated too.
        _fake_db(monkeypatch)
        resp = client.get("/health")
        assert resp.status_code == 503
        assert str(auth_enabled) not in resp.text
        assert "tokens.json" not in resp.text

    def test_fifo_token_store_fails_instead_of_blocking(self, client: TestClient, auth_enabled: Path) -> None:
        # Reading a FIFO on the request path would hang the event loop forever.
        import os as _os

        _os.mkfifo(auth_enabled)
        try:
            resp = client.get("/api/v1/backfill", headers=_bearer("anything"))
            assert resp.status_code == 503
        finally:
            auth_enabled.unlink()

    def test_oversized_token_store_is_rejected(self, client: TestClient, auth_enabled: Path) -> None:
        auth_enabled.write_bytes(b"[" + b" " * (catalog_auth.MAX_TOKEN_FILE_BYTES + 1) + b"]")
        resp = client.get("/api/v1/backfill", headers=_bearer("anything"))
        assert resp.status_code == 503

    def test_health_reports_unhealthy(
        self, client: TestClient, auth_enabled: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _fake_db(monkeypatch)
        resp = client.get("/health")
        assert resp.status_code == 503

    def test_startup_logging_does_not_raise(self, auth_enabled: Path) -> None:
        # lifespan must not kill the process over a repairable file problem.
        catalog_auth.log_auth_state()

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(json.dumps([None]).encode(), id="null_record"),
            pytest.param(b"[\xff]", id="invalid_utf8"),
            pytest.param(b"[" * 20000 + b"]" * 20000, id="recursion_depth"),
        ],
    )
    def test_startup_logging_does_not_raise_on_corrupt_store(self, auth_enabled: Path, content: bytes) -> None:
        auth_enabled.write_bytes(content)
        catalog_auth.log_auth_state()

    def test_startup_logging_does_not_raise_when_disabled(self) -> None:
        catalog_auth.log_auth_state()


class TestComposeContract:
    def test_compose_passes_auth_flag_to_catalog(self) -> None:
        import yaml

        compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
        compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
        env = compose["services"]["heber-catalog"]["environment"]
        assert any(str(entry).startswith("HEBER_CATALOG_AUTH_ENABLED=") for entry in env), (
            "heber-catalog must pass HEBER_CATALOG_AUTH_ENABLED through to the container, "
            "otherwise enabling auth on the host silently does nothing"
        )

    def test_compose_passes_tokens_file_to_catalog(self) -> None:
        import yaml

        compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
        compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
        env = compose["services"]["heber-catalog"]["environment"]
        assert any(str(entry).startswith("HEBER_CATALOG_AUTH_TOKENS_FILE=") for entry in env), (
            "heber-catalog must pass HEBER_CATALOG_AUTH_TOKENS_FILE through, otherwise a custom "
            "token path is honoured on the host but ignored inside the container"
        )
