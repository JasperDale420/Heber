"""Bearer-token auth for the catalog API.

Gated by HEBER_CATALOG_AUTH_ENABLED (default off — requests pass through
untouched). When enabled, every route carrying one of the dependencies below
requires a bearer token that hashes to a persisted token record; token records
are validated through heber.catalog.access_control (SHA-256 hashes only — raw
tokens are printed once by `heber catalog-token create` and never stored).

Scopes gate read versus write across the whole catalog. A token's project_id is
an audit label — it is logged, not enforced — so any write-scoped token can
mutate any dataset. Per-project dataset isolation is not implemented here.

When the token store cannot be read, the service does not exit: /health reports
unhealthy and protected routes return 503, so a bad file mode is a visible,
repairable state rather than a crash loop.
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from stat import S_ISREG
from typing import Any

import structlog
from fastapi import HTTPException, Request

from heber.catalog.access_control import DEFAULT_SCOPES, AccessControlManager, SDKToken
from heber.config import settings

logger = structlog.get_logger(__name__)

# Cached (fingerprint, manager). Any on-disk change — new token, revocation,
# replacement file — invalidates the cache so edits apply without a service
# restart. ctime and inode are in the fingerprint because mtime and size alone
# can be forged: a same-length edit plus a restored mtime would otherwise keep
# a revoked token live.
_cached: tuple[tuple[Any, ...], AccessControlManager] | None = None

# Scopes that satisfy a required scope. Without this an "admin" token is
# rejected everywhere and a "write" token cannot read, which reads as a
# lockout to anyone who picks those scopes from the CLI help text.
_SCOPE_IMPLIES: dict[str, tuple[str, ...]] = {
    "read": ("read", "write", "admin"),
    "write": ("write", "admin"),
}


# A token record is a few hundred bytes; anything past this is corruption, and
# reading it on the request path would be a denial of service.
MAX_TOKEN_FILE_BYTES = 8 * 1024 * 1024

# The catalog container runs as uid 10000, gid 0, so root-group access counts.
_CONTAINER_GID = 0


class TokenStoreUnavailable(RuntimeError):
    """The token records file is missing, unreadable, or malformed."""


def _record_to_token(record: Any) -> SDKToken:
    """Build an SDKToken from a persisted record; fail loud on malformed input.

    Anything hand-edited into the file — a bare `null`, a string, a number where
    a timestamp belongs — must surface as ValueError so the caller can report a
    controlled failure instead of letting a TypeError/AttributeError escape into
    an un-enveloped 500 or a startup crash.
    """
    if not isinstance(record, dict):
        raise ValueError(f"Token record must be a JSON object, got {type(record).__name__}")
    try:
        expires_at = datetime.fromisoformat(record["expires_at"]) if record.get("expires_at") else None
        created_at = datetime.fromisoformat(record["created_at"]) if record.get("created_at") else datetime.now(UTC)
        if expires_at is not None and expires_at.tzinfo is None:
            raise ValueError(f"Token record '{record.get('token_id')}' has a naive expires_at; must be tz-aware")
        scopes = record.get("scopes", [])
        if not isinstance(scopes, list):
            raise ValueError(f"Token record '{record.get('token_id')}' has non-list scopes")
        for field_name in ("token_id", "project_id", "token_hash", "name"):
            if not isinstance(record[field_name], str):
                raise ValueError(f"Token record field '{field_name}' must be a string")
        return SDKToken(
            token_id=record["token_id"],
            project_id=record["project_id"],
            token_hash=record["token_hash"],
            name=record["name"],
            scopes=[str(s) for s in scopes],
            expires_at=expires_at,
            created_at=created_at,
            revoked=bool(record.get("revoked", False)),
        )
    except KeyError as exc:
        token_id = record.get("token_id", "<no token_id>")
        raise ValueError(f"Token record missing required field {exc}: {token_id}") from exc
    except TypeError as exc:
        token_id = record.get("token_id", "<no token_id>")
        raise ValueError(f"Token record '{token_id}' has a field of the wrong type: {exc}") from exc


def load_access_control(tokens_file: Path) -> AccessControlManager:
    """Build an AccessControlManager from persisted token records.

    The file is untrusted input — hand-edited, half-written, or arbitrary bytes.
    Every way it can fail must surface as TokenStoreUnavailable so callers can
    turn it into a controlled 503 instead of an un-enveloped 500 or a dead
    process. The trailing catch-all covers what the specific clauses miss:
    undecodable bytes, a JSON nesting depth that trips the parser's recursion
    limit, and anything else a future edit of this function might introduce.
    """
    if not tokens_file.exists():
        raise TokenStoreUnavailable(
            f"No catalog token file at {tokens_file}. Bootstrap the first token with: "
            "uv run heber catalog-token create --project <project> --name <name> --scopes read,write"
        )
    try:
        return _load_records(tokens_file)
    except TokenStoreUnavailable:
        raise
    except OSError as exc:
        raise TokenStoreUnavailable(
            f"Catalog auth cannot read token file {tokens_file}: {exc}. "
            "Ensure the catalog process user can read it (in Docker the service "
            "runs as uid 10000 — chown/chmod the file on the bind mount accordingly)."
        ) from exc
    except Exception as exc:
        raise TokenStoreUnavailable(
            f"Token file {tokens_file} could not be parsed ({type(exc).__name__}: {exc})"
        ) from exc


def _load_records(tokens_file: Path) -> AccessControlManager:
    # Regular files only, and only plausibly-sized ones. A FIFO or device at this
    # path would block the event loop forever on read instead of failing, and a
    # huge file is corruption, not a token store.
    stat = tokens_file.stat()
    if not S_ISREG(stat.st_mode):
        raise TokenStoreUnavailable(f"Token file {tokens_file} is not a regular file")
    if stat.st_size > MAX_TOKEN_FILE_BYTES:
        raise TokenStoreUnavailable(
            f"Token file {tokens_file} is {stat.st_size} bytes, over the {MAX_TOKEN_FILE_BYTES} byte limit"
        )
    raw_text = tokens_file.read_text()
    try:
        records = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise TokenStoreUnavailable(f"Token file {tokens_file} is not valid JSON: {exc}") from exc
    if not isinstance(records, list):
        raise TokenStoreUnavailable(f"Token file {tokens_file} must contain a JSON list of token records")

    manager = AccessControlManager()
    for record in records:
        try:
            token = _record_to_token(record)
        except ValueError as exc:
            raise TokenStoreUnavailable(f"Token file {tokens_file} has a malformed record: {exc}") from exc
        manager.tokens[token.token_id] = token

    valid = sum(1 for t in manager.tokens.values() if t.is_valid())
    if valid == 0:
        logger.warning(
            "catalog_auth_no_valid_tokens",
            tokens_file=str(tokens_file),
            total_tokens=len(manager.tokens),
            detail="every request will be rejected until a valid token is created",
        )
    return manager


def _fingerprint(tokens_file: Path, stat: os.stat_result) -> tuple[Any, ...]:
    """Identity of the on-disk token store, for cache invalidation."""
    return (tokens_file, stat.st_ino, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size)


def get_access_control() -> AccessControlManager:
    """Manager built from settings, reloaded automatically when the file changes."""
    global _cached
    tokens_file = settings.catalog_auth_tokens_path
    try:
        stat = tokens_file.stat()
    except OSError as exc:
        _cached = None
        raise TokenStoreUnavailable(
            f"Catalog token file at {tokens_file} is unavailable ({exc}). Bootstrap the first token with: "
            "uv run heber catalog-token create --project <project> --name <name> --scopes read,write"
        ) from None

    fingerprint = _fingerprint(tokens_file, stat)
    if _cached is not None:
        cached_fingerprint, manager = _cached
        if cached_fingerprint == fingerprint:
            return manager

    manager = load_access_control(tokens_file)
    _cached = (fingerprint, manager)
    logger.info(
        "catalog_auth_tokens_loaded",
        tokens_file=str(tokens_file),
        total_tokens=len(manager.tokens),
        valid_tokens=sum(1 for t in manager.tokens.values() if t.is_valid()),
    )
    return manager


def reset_access_control() -> None:
    """Drop the cached manager (tests; file changes reload automatically)."""
    global _cached
    _cached = None


def log_auth_state() -> None:
    """Log the auth state at startup. Never raises.

    An unloadable token store does not kill the process: under `restart: always`
    that turns a fixable file-permission mistake into a crash loop with no way
    to inspect or repair the container. Instead the service stays up, /health
    reports unhealthy, and every protected route returns 503 until the store
    loads — fail-closed and visible rather than fail-closed and invisible.
    """
    if not settings.catalog_auth_enabled:
        logger.warning(
            "catalog_auth_disabled",
            detail="all catalog endpoints accept unauthenticated requests; "
            "set HEBER_CATALOG_AUTH_ENABLED=true to require bearer tokens",
        )
        return
    try:
        manager = get_access_control()
    except Exception as exc:
        # Deliberately broad: startup logging must never be the reason the
        # process dies, whatever the token file turns out to contain.
        logger.error(
            "catalog_auth_token_store_unavailable",
            tokens_file=str(settings.catalog_auth_tokens_path),
            error=str(exc),
            detail="auth is enabled but no usable token store was found; /health is unhealthy "
            "and every protected route returns 503 until this is fixed",
            exc_info=True,
        )
        return
    logger.info(
        "catalog_auth_enabled",
        tokens_file=str(settings.catalog_auth_tokens_path),
        total_tokens=len(manager.tokens),
        valid_tokens=sum(1 for t in manager.tokens.values() if t.is_valid()),
    )


def auth_health() -> dict[str, Any] | None:
    """Auth visibility for /health: None when disabled, else enabled + valid count.

    Raises TokenStoreUnavailable when auth is on but the token store cannot be
    read, so /health can report unhealthy instead of showing a green container
    whose every data route is failing.
    """
    if not settings.catalog_auth_enabled:
        return None
    manager = get_access_control()
    valid = sum(1 for t in manager.tokens.values() if t.is_valid())
    if valid == 0:
        logger.warning("catalog_auth_no_valid_tokens", detail="all requests are being rejected")
    return {"enabled": True, "valid_tokens": valid}


def _extract_bearer(request: Request) -> str | None:
    header = request.headers.get("Authorization")
    if not header:
        return None
    scheme, _, credentials = header.partition(" ")
    if scheme.lower() != "bearer" or not credentials.strip():
        return None
    return credentials.strip()


def _unauthorized(reason: str, request: Request) -> HTTPException:
    logger.warning("catalog_auth_rejected", reason=reason, path=request.url.path)
    return HTTPException(
        status_code=401,
        detail="Missing or invalid bearer token",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _has_required_scope(token: SDKToken, scope: str) -> bool:
    """True when the token carries the required scope or one that implies it."""
    return any(token.has_scope(granting) for granting in _SCOPE_IMPLIES.get(scope, (scope,)))


def _authenticate(request: Request, scope: str) -> SDKToken | None:
    if not settings.catalog_auth_enabled:
        return None
    # Store availability is checked before the header so an outage reports as an
    # outage (503) for every protected request, not as a client error (401) that
    # hides it from unauthenticated probes.
    try:
        manager = get_access_control()
    except TokenStoreUnavailable as exc:
        # 503, not 500: the request is well-formed, the server cannot check it.
        # The detail (file path, OS error, parser message) goes to the log only —
        # this response reaches unauthenticated callers.
        logger.error("catalog_auth_token_store_unavailable", path=request.url.path, error=str(exc))
        raise HTTPException(
            status_code=503,
            detail="Catalog auth is enabled but the token store is unavailable",
        ) from exc
    raw = _extract_bearer(request)
    if raw is None:
        raise _unauthorized("missing_bearer_token", request)
    token = manager.validate_token(raw)
    if token is None:
        raise _unauthorized("invalid_or_expired_token", request)
    if not _has_required_scope(token, scope):
        logger.warning(
            "catalog_auth_forbidden",
            token_id=token.token_id,
            project_id=token.project_id,
            required_scope=scope,
            path=request.url.path,
        )
        raise HTTPException(status_code=403, detail=f"Token lacks required scope '{scope}'")
    return token


async def require_read(request: Request) -> SDKToken | None:
    """Route dependency: no-op when auth is disabled, else require read scope."""
    return _authenticate(request, "read")


async def require_write(request: Request) -> SDKToken | None:
    """Route dependency: no-op when auth is disabled, else require write scope."""
    return _authenticate(request, "write")


def create_token_record(
    tokens_file: Path,
    project_id: str,
    name: str,
    scopes: list[str] | None = None,
    expires_in_days: int | None = None,
) -> tuple[str, SDKToken]:
    """Mint a token, append its hashed record to tokens_file, return (raw, token).

    The raw token is returned exactly once — only the SHA-256 hash is persisted.
    The append is serialized via a lock file and lands via atomic rename, so a
    crash mid-write can never leave a corrupt token file behind.

    The file lands world-readable (0644). The catalog runs in Docker as uid
    10000 while this CLI runs as the host user on the same bind mount, so an
    owner-only file is unreadable by the service and would take the whole
    catalog down. The file holds SHA-256 hashes and metadata only — never a
    usable credential — so read access to it does not grant catalog access.
    """
    if scopes:
        unknown = [s for s in scopes if s not in DEFAULT_SCOPES]
        if unknown:
            raise ValueError(f"Unknown scopes {unknown}; allowed: {sorted(DEFAULT_SCOPES)}")
    if expires_in_days is not None and expires_in_days <= 0:
        raise ValueError(f"expires_in_days must be positive, got {expires_in_days}")

    manager = AccessControlManager()
    raw, token = manager.create_token(project_id, name, scopes=scopes, expires_in_days=expires_in_days)

    # Only a directory this call creates gets the traversable mode the catalog
    # container needs; an existing directory keeps whatever the operator chose,
    # since relaxing it could expose unrelated files alongside the token store.
    if not tokens_file.parent.exists():
        tokens_file.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(tokens_file.parent, 0o755)
    lock_path = tokens_file.parent / (tokens_file.name + ".lock")
    with open(lock_path, "w") as lock_handle:
        fcntl.flock(lock_handle, fcntl.LOCK_EX)

        records: list[dict[str, Any]] = []
        if tokens_file.exists():
            try:
                records = json.loads(tokens_file.read_text())
            except json.JSONDecodeError as exc:
                raise ValueError(f"Token file {tokens_file} is not valid JSON: {exc}") from exc
            if not isinstance(records, list):
                raise ValueError(f"Token file {tokens_file} must contain a JSON list of token records")

        records.append(
            {
                "token_id": token.token_id,
                "project_id": token.project_id,
                "token_hash": token.token_hash,
                "name": token.name,
                "scopes": token.scopes,
                "expires_at": token.expires_at.isoformat() if token.expires_at else None,
                "created_at": token.created_at.isoformat(),
                "revoked": token.revoked,
            }
        )

        fd, tmp_path = tempfile.mkstemp(dir=tokens_file.parent, prefix=tokens_file.name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as tmp_handle:
                tmp_handle.write(json.dumps(records, indent=2) + "\n")
                tmp_handle.flush()
                os.fsync(tmp_handle.fileno())
            os.chmod(tmp_path, 0o644)
            os.replace(tmp_path, tokens_file)
        except BaseException:
            os.unlink(tmp_path)
            raise

    logger.info(
        "catalog_auth_token_created",
        token_id=token.token_id,
        project_id=project_id,
        scopes=token.scopes,
        tokens_file=str(tokens_file),
    )
    warning = unreachable_by_container(tokens_file)
    if warning:
        logger.warning("catalog_auth_token_file_unreachable_by_container", tokens_file=str(tokens_file), detail=warning)
    return raw, token


def _reachable(stat: os.stat_result, bit: int) -> bool:
    """True when uid 10000 / gid 0 would get this permission bit.

    `bit` is the "other" bit (0o001 execute, 0o004 read); the group equivalent is
    checked too, because the container's gid 0 matches root-group-owned paths.
    """
    if stat.st_mode & bit:
        return True
    return stat.st_gid == _CONTAINER_GID and bool(stat.st_mode & (bit << 3))


def _checked_ancestors(tokens_file: Path) -> list[Path]:
    """Directories between the token file and the bind-mount root, inclusive.

    Everything from data_root down is visible inside the container, so a
    restrictive mode anywhere in that span blocks it. Paths above data_root are
    replaced by the mount and say nothing about the container's view.
    """
    directories = [tokens_file.parent]
    data_root = settings.data_root
    for parent in tokens_file.parent.parents:
        if parent in directories:
            continue
        directories.append(parent)
        if parent == data_root:
            break
    else:
        # Not under data_root — only the immediate parent is meaningful.
        return [tokens_file.parent]
    return directories


def unreachable_by_container(tokens_file: Path) -> str | None:
    """Explain why the catalog container could not read this file, or None.

    Minting a token into a directory the operator locked down succeeds on the
    host and then leaves the catalog stuck on 503, because the container runs as
    a different uid. Say so at mint time instead of letting it be discovered
    later; the fix is the operator's to make, since widening their directory
    could expose whatever else lives in it.

    This is a best-effort hint from the host's view, not proof: it cannot see
    ACLs, and Docker Desktop's file sharing may translate ownership. Treat a
    warning as "check this", and its absence as "no obvious problem".
    """
    blockers = []
    try:
        for directory in _checked_ancestors(tokens_file):
            if not _reachable(directory.stat(), 0o001):
                blockers.append(f"{directory} is not traversable by uid 10000 (needs o+x, or g+x owned by gid 0)")
        if not _reachable(tokens_file.stat(), 0o004):
            blockers.append(f"{tokens_file} is not readable by uid 10000 (needs o+r, or g+r owned by gid 0)")
    except OSError:
        return None
    if not blockers:
        return None
    return "; ".join(blockers) + ". The catalog container will report 503 until it can read this file."


def list_token_records(tokens_file: Path) -> list[dict[str, Any]]:
    """Return token metadata (no hashes) for every persisted record."""
    manager = load_access_control(tokens_file)
    return [token.to_dict() for token in manager.tokens.values()]
