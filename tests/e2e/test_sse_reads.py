"""Read-only SSE batches preserve authorization, cursors and writer ordering."""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from threading import Event as ThreadEvent
from types import SimpleNamespace

import pytest
from sqlalchemy import event, func, select, text
from test_web import web

from aftercare.api import routes
from aftercare.domain.contracts import BusinessError, utc_now
from aftercare.repositories.models import Application, DemoSession, Event, Job
from aftercare.services.business import columns, digest
from aftercare.services.event_reads import EventReader

pytestmark = pytest.mark.integration


@pytest.fixture
def sse(service):
    conversation = service.create_conversation("alice")
    token = "D2-local-test-session-token"
    session_hash = digest(token)
    with service.sessions.begin() as session:
        session.add(
            DemoSession(
                token_hash=session_hash,
                customer_id="alice",
                csrf_token="D2-local-test-csrf",
                expires_at=utc_now() + timedelta(hours=1),
            )
        )
    return SimpleNamespace(
        service=service,
        conversation=conversation,
        token=token,
        session_hash=session_hash,
        reader=EventReader(service.sessions),
    )


def read(context, after=0, **overrides):
    return context.reader.read_batch(
        overrides.get("principal", "alice"),
        overrides.get("conversation_id", context.conversation),
        after,
        overrides.get("session_hash", context.session_hash),
    )


def append(context, count, conversation=None):
    service, conversation = context.service, conversation or context.conversation
    with service.sessions.begin() as session:
        service._conversation(session, "alice", conversation)
        for index in range(count):
            service.event(
                session, conversation, "synthetic_trace", {"index": index, "text": "完整"}
            )


@contextmanager
def statements(context):
    calls = []
    engine = context.service.sessions.kw["bind"]

    def record(connection, cursor, statement, parameters, execute_context, many):
        calls.append(" ".join(statement.lower().split()))

    event.listen(engine, "before_cursor_execute", record)
    try:
        yield calls
    finally:
        event.remove(engine, "before_cursor_execute", record)


def frame_ids(body):
    return [int(line[4:]) for line in body.splitlines() if line.startswith("id: ")]


def forbidden(*args, **kwargs):
    raise AssertionError("SSE_MUST_NOT_USE_INTERNAL_SNAPSHOT_OR_EXECUTION_LOCK")


def test_batch_is_bounded_and_replays_all_original_event_fields(sse):
    append(sse, 204)
    with sse.service.sessions() as session:
        expected = [
            columns(row)
            for row in session.scalars(
                select(Event).where(Event.conversation_id == sse.conversation).order_by(Event.id)
            )
        ]
    first, second = read(sse), read(sse, expected[99]["id"])
    third = read(sse, expected[199]["id"])
    assert [len(first), len(second), len(third)] == [100, 100, 5]
    assert first + second + third == expected
    assert read(sse, expected[-1]["id"]) == []
    assert read(sse, 2**63 - 1) == []


def test_batch_uses_three_read_only_statements_and_no_execution_guard(sse, monkeypatch):
    monkeypatch.setattr(sse.service, "_conversation", forbidden)
    monkeypatch.setattr(sse.service, "snapshot", forbidden)
    sse.service.run_guard = forbidden
    with statements(sse) as sql:
        rows = read(sse)
    assert rows and len(sql) == 3
    assert "repeatable read" in sql[0] and "read only" in sql[0]
    assert all("for update" not in value and "for share" not in value for value in sql)
    assert not any("action_previews" in value or "from messages" in value for value in sql)


@pytest.mark.parametrize("change", ["expired", "deleted", "principal_mismatch", "wrong_hash"])
def test_batch_revalidates_identity_before_reading_events(sse, change):
    overrides = {}
    if change in {"expired", "deleted"}:
        with sse.service.sessions.begin() as session:
            row = session.get(DemoSession, sse.session_hash)
            if change == "deleted":
                session.delete(row)
            else:
                row.expires_at = utc_now() - timedelta(seconds=1)
    elif change == "principal_mismatch":
        overrides["principal"] = "bob"
    else:
        overrides["session_hash"] = digest("missing-session")
    with statements(sse) as sql, pytest.raises(BusinessError) as error:
        read(sse, **overrides)
    assert error.value.code == "SESSION_EXPIRED"
    assert len(sql) == 2 and not any("from events" in value for value in sql)


@pytest.mark.parametrize("kind", ["other_owner", "missing"])
def test_batch_checks_conversation_ownership_before_event_query(sse, kind):
    other = sse.service.create_conversation("bob") if kind == "other_owner" else "missing"
    with statements(sse) as sql, pytest.raises(BusinessError) as error:
        read(sse, conversation_id=other)
    assert error.value.code == "NOT_FOUND"
    assert len(sql) == 2 and not any("from events" in value for value in sql)


def test_production_writer_lock_keeps_same_conversation_event_cursor_prefix(sse):
    service, started, ids = sse.service, ThreadEvent(), {}
    original = read(sse)
    last = original[-1]["id"]
    other = service.create_conversation("alice")

    def second_writer():
        with service.sessions.begin() as session:
            ids["pid"] = session.scalar(text("SELECT pg_backend_pid()"))
            started.set()
            service._conversation(session, "alice", sse.conversation)
            row = service.event(session, sse.conversation, "second_writer", {})
            session.flush()
            ids["second"] = row.id
        return ids["second"]

    with ThreadPoolExecutor(max_workers=1) as pool:
        with service.sessions.begin() as first:
            service._conversation(first, "alice", sse.conversation)
            low = service.event(first, sse.conversation, "first_writer", {})
            first.flush()
            low_id = low.id
            future = pool.submit(second_writer)
            assert started.wait(2)
            deadline, blocked = time.monotonic() + 2, False
            while time.monotonic() < deadline:
                with service.sessions() as observer:
                    blocked = bool(
                        observer.scalar(
                            text("SELECT cardinality(pg_blocking_pids(:pid))"),
                            {"pid": ids["pid"]},
                        )
                    )
                if blocked:
                    break
                time.sleep(0.01)
            assert blocked and "second" not in ids
            # Global IDs may advance in another conversation; that cannot move our cursor.
            append(sse, 1, conversation=other)
            assert read(sse, last) == []
        high_id = future.result(timeout=5)
    replay = read(sse, last)
    assert [row["id"] for row in replay] == [low_id, high_id]
    assert low_id < high_id
    assert [row["event_type"] for row in replay] == ["first_writer", "second_writer"]


def test_sse_once_keeps_wire_contract_cursor_precedence_and_no_side_effects(service, monkeypatch):
    client, _ = web(service)
    conversation = client.post("/api/conversations").json()["id"]
    context = SimpleNamespace(service=service, conversation=conversation)
    append(context, 204)
    monkeypatch.setattr(service, "snapshot", forbidden)
    monkeypatch.setattr(service, "_conversation", forbidden)
    with service.sessions() as session:
        expected = list(
            session.scalars(
                select(Event.id).where(Event.conversation_id == conversation).order_by(Event.id)
            )
        )
        before = {
            model.__tablename__: session.scalar(select(func.count()).select_from(model))
            for model in (Event, Application, Job)
        }
    url = f"/api/conversations/{conversation}/events?once=true"
    response = client.get(url)
    assert response.status_code == 200 and frame_ids(response.text) == expected[:100]
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"
    assert ": heartbeat" not in response.text and "event: trace" in response.text
    replay = client.get(
        url + f"&after={expected[49]}", headers={"Last-Event-ID": str(expected[99])}
    )
    assert frame_ids(replay.text) == expected[100:200]
    assert frame_ids(client.get(url, headers={"Last-Event-ID": str(expected[-1])}).text) == []
    with service.sessions() as session:
        assert {
            model.__tablename__: session.scalar(select(func.count()).select_from(model))
            for model in (Event, Application, Job)
        } == before


@pytest.mark.parametrize("header", ["not-an-id", str(2**63)])
def test_sse_rejects_invalid_header_cursor_before_starting_stream(service, header):
    client, _ = web(service)
    conversation = client.post("/api/conversations").json()["id"]
    response = client.get(
        f"/api/conversations/{conversation}/events?once=true", headers={"Last-Event-ID": header}
    )
    assert response.status_code == 400 and response.json()["error_code"] == "INVALID_EVENT_CURSOR"


class StreamRequest:
    def __init__(self, context):
        self.app = SimpleNamespace(state=SimpleNamespace(business=context.service))
        self.headers = {}
        self.cookies = {routes.COOKIE: context.token}
        self.disconnected = False

    async def is_disconnected(self):
        return self.disconnected


async def start_stream(context, request):
    return await routes.events(
        context.conversation,
        request,
        after=0,
        once=False,
        who={"principal": "alice", "csrf_token": "not-needed-for-GET"},
    )


def test_slow_stream_and_disconnect_do_not_hold_database_connection(sse):
    async def scenario():
        request = StreamRequest(sse)
        response = await start_stream(sse, request)
        pool = sse.service.sessions.kw["bind"].pool
        assert pool.checkedout() == 0
        iterator = response.body_iterator
        assert "event: trace" in await anext(iterator)
        assert pool.checkedout() == 0
        request.disconnected = True
        async for _ in iterator:
            pass
        assert pool.checkedout() == 0

    asyncio.run(scenario())


def test_full_backlog_batches_are_drained_without_idle_delay(sse, monkeypatch):
    append(sse, 204)
    real_sleep, delays = asyncio.sleep, []

    async def sleep(delay):
        delays.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(routes.asyncio, "sleep", sleep)

    async def scenario():
        request = StreamRequest(sse)
        response = await start_stream(sse, request)
        iterator, received = response.body_iterator, []
        while len(received) < 205:
            value = await anext(iterator)
            received.extend(frame_ids(value))
        request.disconnected = True
        await iterator.aclose()
        assert len(received) == len(set(received)) == 205
        assert not any(delay > 0 for delay in delays)

    asyncio.run(scenario())


def test_idle_poll_backoff_is_bounded_and_disconnect_stops_reads(sse, monkeypatch):
    request, delays, reads = StreamRequest(sse), [], []
    real_sleep = asyncio.sleep

    def empty(self, *args, **kwargs):
        reads.append(1)
        return []

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 4:
            request.disconnected = True
        await real_sleep(0)

    monkeypatch.setattr(EventReader, "read_batch", empty)
    monkeypatch.setattr(routes.asyncio, "sleep", sleep)

    async def scenario():
        response = await start_stream(sse, request)
        async for frame in response.body_iterator:
            assert frame == ": heartbeat\n\n"

    asyncio.run(scenario())
    assert delays == [0.5, 1.0, 2.0, 2.0] and len(reads) == 4


@pytest.mark.parametrize("code", ["SESSION_EXPIRED", "NOT_FOUND"])
def test_live_authorization_loss_is_terminal_and_does_not_advance_cursor(sse, monkeypatch, code):
    reads = []
    real_sleep = asyncio.sleep

    def expire(self, *args, **kwargs):
        reads.append(1)
        if len(reads) == 1:
            return []
        raise BusinessError(code)

    async def sleep(delay):
        await real_sleep(0)

    monkeypatch.setattr(EventReader, "read_batch", expire)
    monkeypatch.setattr(routes.asyncio, "sleep", sleep)

    async def scenario():
        response = await start_stream(sse, StreamRequest(sse))
        frames = [frame async for frame in response.body_iterator]
        assert frames[0] == ": heartbeat\n\n"
        terminal = frames[-1]
        assert "event: stream_closed\n" in terminal and "id:" not in terminal
        data = json.loads(
            next(line[6:] for line in terminal.splitlines() if line.startswith("data:"))
        )
        assert data["error_code"] == code

    asyncio.run(scenario())
    assert len(reads) == 2
