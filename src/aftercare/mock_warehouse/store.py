"""Independent SQLite warehouse facts, with transactional persistent idempotency."""

import hashlib
import json
import os
import sqlite3
import stat
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

from aftercare.domain.warehouse import (
    PROVIDER_ID,
    ShipmentInput,
    ShipmentResult,
    WarehouseEvent,
    verify_shipment_hash,
)

APPLICATION_ID = 0x41465731
SCHEMA_VERSION = 2
SHIPMENT_COLUMNS = {
    "effect_key",
    "application_id",
    "request_hash",
    "payload_json",
    "external_shipment_id",
    "created_at",
}
EVENT_COLUMNS = {
    "event_id",
    "effect_key",
    "sequence",
    "payload_json",
    "delivery_status",
    "attempts",
    "next_attempt_at",
    "lease_owner",
    "generation",
    "lease_expires_at",
    "last_error_code",
    "created_at",
    "updated_at",
    "delivered_at",
}
EVENT_TRIGGER = "callback_events_immutable"
MAX_CALLBACK_ATTEMPTS = 5


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


class WarehouseStateError(RuntimeError):
    def __init__(self):
        super().__init__("WAREHOUSE_STATE_INVALID")


class WarehouseConflict(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


class WarehouseInvalidRequest(Exception):
    def __init__(self):
        super().__init__("INVALID_REQUEST_HASH")


class WarehouseStore:
    def __init__(self, state_path):
        self.path = Path(state_path).expanduser().absolute()
        created = False
        try:
            if self.path.is_symlink():
                raise WarehouseStateError()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if not self.path.exists():
                if any(
                    Path(str(self.path) + suffix).exists()
                    for suffix in ("-wal", "-shm", "-journal")
                ):
                    raise WarehouseStateError()
                try:
                    fd = os.open(
                        self.path,
                        os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                        0o600,
                    )
                except FileExistsError:
                    self._identity = self._file_identity()
                else:
                    try:
                        info = os.fstat(fd)
                        self._identity = (info.st_dev, info.st_ino)
                        created = True
                    finally:
                        os.close(fd)
            else:
                self._identity = self._file_identity()
            if created:
                self._initialize()
            version = self._validate_existing()
            if version == 1:
                self._migrate_v1()
                self._validate_existing()
            with self._connection() as connection:
                if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                    raise WarehouseStateError()
        except (OSError, sqlite3.Error, ValueError, TypeError, WarehouseStateError):
            if created:
                try:
                    if self._file_identity() == self._identity:
                        for suffix in ("-wal", "-shm", "-journal", ""):
                            Path(str(self.path) + suffix).unlink(missing_ok=True)
                except (OSError, WarehouseStateError):
                    pass
            raise WarehouseStateError() from None

    def _file_identity(self):
        info = self.path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise WarehouseStateError()
        return info.st_dev, info.st_ino

    def _check_identity(self):
        if self._file_identity() != self._identity:
            raise WarehouseStateError()

    def _uri(self, mode):
        return "file:" + quote(str(self.path), safe="/") + "?mode=" + mode

    @contextmanager
    def _connection(self, *, readonly=False):
        self._check_identity()
        connection = sqlite3.connect(
            self._uri("ro" if readonly else "rw"),
            uri=True,
            timeout=5,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute("PRAGMA foreign_keys=ON")
            yield connection
        finally:
            if connection.in_transaction:
                connection.rollback()
            connection.close()

    def _initialize(self):
        with self._connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE provider_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
                "marker TEXT NOT NULL)"
            )
            connection.execute("INSERT INTO provider_meta VALUES (1, ?)", (PROVIDER_ID,))
            connection.execute(
                "CREATE TABLE shipments (effect_key TEXT PRIMARY KEY NOT NULL, "
                "application_id TEXT NOT NULL UNIQUE, request_hash TEXT NOT NULL, "
                "payload_json TEXT NOT NULL, external_shipment_id TEXT NOT NULL UNIQUE, "
                "created_at TEXT NOT NULL)"
            )
            self._create_event_schema(connection)
            connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
            connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            connection.execute("COMMIT")
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def _validate_existing(self):
        # Reject foreign files before SQLite can change journal mode or create sidecars.
        self._check_identity()
        fd = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            header = os.read(fd, 100)
        finally:
            os.close(fd)
        if (
            len(header) < 100
            or header[:16] != b"SQLite format 3\x00"
            or int.from_bytes(header[68:72], "big") != APPLICATION_ID
            or int.from_bytes(header[60:64], "big") not in {1, SCHEMA_VERSION}
        ):
            raise WarehouseStateError()
        with self._connection(readonly=True) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in {1, SCHEMA_VERSION}:
                raise WarehouseStateError()
            marker = connection.execute("SELECT singleton, marker FROM provider_meta").fetchall()
            if [(row[0], row[1]) for row in marker] != [(1, PROVIDER_ID)]:
                raise WarehouseStateError()
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*'"
                )
            }
            expected_tables = {"provider_meta", "shipments"}
            if version == SCHEMA_VERSION:
                expected_tables.add("callback_events")
            if tables != expected_tables:
                raise WarehouseStateError()
            columns = {row[1] for row in connection.execute("PRAGMA table_info(shipments)")}
            if columns != SHIPMENT_COLUMNS:
                raise WarehouseStateError()
            unique_columns = set()
            for index in connection.execute("PRAGMA index_list(shipments)").fetchall():
                if index[2]:
                    name = index[1].replace('"', '""')
                    unique_columns.add(
                        tuple(row[2] for row in connection.execute(f'PRAGMA index_info("{name}")'))
                    )
            if (
                not {("effect_key",), ("application_id",), ("external_shipment_id",)}
                <= unique_columns
            ):
                raise WarehouseStateError()
            triggers = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
            }
            if triggers != ({EVENT_TRIGGER} if version == SCHEMA_VERSION else set()):
                raise WarehouseStateError()
            if version == SCHEMA_VERSION:
                columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(callback_events)")
                }
                if columns != EVENT_COLUMNS:
                    raise WarehouseStateError()
                unique = set()
                for index in connection.execute("PRAGMA index_list(callback_events)").fetchall():
                    if index[2]:
                        name = index[1].replace('"', '""')
                        unique.add(
                            tuple(
                                row[2] for row in connection.execute(f'PRAGMA index_info("{name}")')
                            )
                        )
                if not {("event_id",), ("effect_key", "sequence")} <= unique:
                    raise WarehouseStateError()
            return version

    @staticmethod
    def _create_event_schema(connection):
        connection.execute(
            "CREATE TABLE callback_events (event_id TEXT PRIMARY KEY NOT NULL, "
            "effect_key TEXT NOT NULL REFERENCES shipments(effect_key), "
            "sequence INTEGER NOT NULL CHECK(sequence IN (1,2)), payload_json TEXT NOT NULL, "
            "delivery_status TEXT NOT NULL DEFAULT 'PENDING' "
            "CHECK(delivery_status IN ('PENDING','CLAIMED','DELIVERED','DEAD')), "
            "attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0), "
            "next_attempt_at REAL NOT NULL, lease_owner TEXT, "
            "generation INTEGER NOT NULL DEFAULT 0, "
            "lease_expires_at REAL, last_error_code TEXT, created_at REAL NOT NULL, "
            "updated_at REAL NOT NULL, delivered_at REAL, UNIQUE(effect_key,sequence))"
        )
        connection.execute(
            "CREATE INDEX callback_events_due ON callback_events "
            "(delivery_status,next_attempt_at,created_at,sequence)"
        )
        connection.execute(
            f"CREATE TRIGGER {EVENT_TRIGGER} BEFORE UPDATE OF "
            "event_id,effect_key,sequence,payload_json,created_at ON callback_events "
            "BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CALLBACK_EVENT'); END"
        )

    def _migrate_v1(self):
        # Only a validated provider v1 is ever opened for migration. All facts and the
        # version change commit together; failed backfill leaves the v1 facts intact.
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version == 1:
                self._create_event_schema(connection)
                for row in connection.execute("SELECT * FROM shipments").fetchall():
                    request = ShipmentInput.model_validate(
                        {**json.loads(row["payload_json"]), "request_hash": row["request_hash"]}
                    )
                    if (
                        not verify_shipment_hash(request)
                        or request.effect_key != row["effect_key"]
                        or request.application_id != row["application_id"]
                    ):
                        raise WarehouseStateError()
                    self._insert_events(connection, self._result(row))
                connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            elif version != SCHEMA_VERSION:
                raise WarehouseStateError()
            connection.execute("COMMIT")
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    @staticmethod
    def _insert_events(connection, result):
        now = time.time()
        for sequence, status in ((1, "ACCEPTED"), (2, "SUCCEEDED")):
            identity = canonical_json(
                {
                    "provider": PROVIDER_ID,
                    "effect_key": result.effect_key,
                    "request_hash": result.request_hash,
                    "sequence": sequence,
                }
            )
            event = WarehouseEvent(
                event_id="event:" + hashlib.sha256(identity.encode()).hexdigest(),
                effect_key=result.effect_key,
                application_id=result.application_id,
                request_hash=result.request_hash,
                external_shipment_id=result.external_shipment_id,
                sequence=sequence,
                status=status,
            )
            connection.execute(
                "INSERT INTO callback_events "
                "(event_id,effect_key,sequence,payload_json,next_attempt_at,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    event.event_id,
                    event.effect_key,
                    sequence,
                    canonical_json(event.model_dump()),
                    now,
                    now,
                    now,
                ),
            )

    @staticmethod
    def _result(row):
        return ShipmentResult(
            effect_key=row["effect_key"],
            application_id=row["application_id"],
            request_hash=row["request_hash"],
            external_shipment_id=row["external_shipment_id"],
            status="SUCCEEDED",
        )

    def ready(self):
        with self._connection(readonly=True) as connection:
            row = connection.execute(
                "SELECT marker FROM provider_meta WHERE singleton=1"
            ).fetchone()
            return row is not None and row[0] == PROVIDER_ID

    def get(self, effect_key):
        with self._connection(readonly=True) as connection:
            row = connection.execute(
                "SELECT * FROM shipments WHERE effect_key=?", (effect_key,)
            ).fetchone()
            return self._result(row) if row is not None else None

    def events(self, effect_key):
        """Immutable provider facts, in business sequence order; no delivery metadata."""
        with self._connection(readonly=True) as connection:
            rows = connection.execute(
                "SELECT payload_json FROM callback_events WHERE effect_key=? ORDER BY sequence",
                (effect_key,),
            ).fetchall()
            return [WarehouseEvent.model_validate_json(row[0]).model_dump() for row in rows]

    def claim_event(self, owner, *, effect_key=None, lease_seconds=10.0, now=None):
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            # SQLite may wait for another writer. Production leases start only
            # once that wait ends; explicit now remains a fixed test clock.
            now = time.time() if now is None else now
            # An expired sender can only repeat the same immutable event. It can
            # never consume the replacement sender's generation on completion.
            connection.execute(
                "UPDATE callback_events SET delivery_status=CASE WHEN attempts>=? "
                "THEN 'DEAD' ELSE 'PENDING' END, lease_owner=NULL, lease_expires_at=NULL, "
                "last_error_code='CALLBACK_LEASE_EXPIRED',updated_at=? "
                "WHERE delivery_status='CLAIMED' AND lease_expires_at<=?",
                (MAX_CALLBACK_ATTEMPTS, now, now),
            )
            predicate = " AND effect_key=?" if effect_key is not None else ""
            params = (MAX_CALLBACK_ATTEMPTS, now) + (
                (effect_key,) if effect_key is not None else ()
            )
            row = connection.execute(
                "SELECT * FROM callback_events WHERE delivery_status='PENDING' "
                "AND attempts<? AND next_attempt_at<=?"
                + predicate
                + " ORDER BY created_at,effect_key,sequence LIMIT 1",
                params,
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            event = WarehouseEvent.model_validate_json(row["payload_json"]).model_dump()
            connection.execute(
                "UPDATE callback_events SET delivery_status='CLAIMED',attempts=attempts+1, "
                "lease_owner=?,generation=generation+1,lease_expires_at=?,updated_at=? "
                "WHERE event_id=?",
                (owner, now + lease_seconds, now, row["event_id"]),
            )
            connection.execute("COMMIT")
            return {
                "event": event,
                "owner": owner,
                "generation": row["generation"] + 1,
                "attempts": row["attempts"] + 1,
            }

    def finish_event(self, claim, *, acknowledged, now=None):
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            # Recheck expiry against lock-acquisition time, not a timestamp
            # captured before a potentially long SQLite write-lock wait.
            now = time.time() if now is None else now
            row = connection.execute(
                "SELECT * FROM callback_events WHERE event_id=? AND delivery_status='CLAIMED' "
                "AND lease_owner=? AND generation=? AND lease_expires_at>?",
                (claim["event"]["event_id"], claim["owner"], claim["generation"], now),
            ).fetchone()
            if row is None or row["payload_json"] != canonical_json(claim["event"]):
                connection.execute("COMMIT")
                return None
            status = (
                "DELIVERED"
                if acknowledged
                else "DEAD"
                if row["attempts"] >= MAX_CALLBACK_ATTEMPTS
                else "PENDING"
            )
            next_at = now + min(30, 2 ** (row["attempts"] - 1))
            connection.execute(
                "UPDATE callback_events SET delivery_status=?,next_attempt_at=?,lease_owner=NULL, "
                "lease_expires_at=NULL,last_error_code=?,updated_at=?,delivered_at=? "
                "WHERE event_id=?",
                (
                    status,
                    next_at,
                    None if acknowledged else "CALLBACK_NOT_ACKNOWLEDGED",
                    now,
                    now if acknowledged else None,
                    row["event_id"],
                ),
            )
            connection.execute("COMMIT")
            return status

    def _insert_shipment(self, connection, request, payload):
        connection.execute(
            "INSERT INTO shipments "
            "(effect_key, application_id, request_hash, payload_json, "
            "external_shipment_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                request.effect_key,
                request.application_id,
                request.request_hash,
                payload,
                "MOCK-" + request.request_hash,
                datetime.now(UTC).isoformat(),
            ),
        )

    def accept(self, request: ShipmentInput, *, before_accept: Callable[[], None] | None = None):
        if not isinstance(request, ShipmentInput) or not verify_shipment_hash(request):
            raise WarehouseInvalidRequest()
        payload = canonical_json(request.request_payload())
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM shipments WHERE effect_key=?", (request.effect_key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["request_hash"] != request.request_hash
                    or existing["payload_json"] != payload
                ):
                    raise WarehouseConflict("IDEMPOTENCY_CONFLICT")
                connection.execute("COMMIT")
                return self._result(existing), False
            if (
                connection.execute(
                    "SELECT 1 FROM shipments WHERE application_id=?", (request.application_id,)
                ).fetchone()
                is not None
            ):
                raise WarehouseConflict("APPLICATION_CONFLICT")
            if before_accept is not None:
                before_accept()
            self._insert_shipment(connection, request, payload)
            row = connection.execute(
                "SELECT * FROM shipments WHERE effect_key=?", (request.effect_key,)
            ).fetchone()
            result = self._result(row)
            self._insert_events(connection, result)
            connection.execute("COMMIT")
            return result, True
