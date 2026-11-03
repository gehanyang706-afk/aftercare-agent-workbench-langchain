"""Real PostgreSQL pages preserve complete rows, scope and legacy read contracts."""

from datetime import timedelta

import pytest
from sqlalchemy import select
from test_fulfillment_api import api as api
from test_fulfillment_api import customer, error, post, prepared, staff, success

from aftercare.domain.fulfillment import Actor
from aftercare.repositories.models import Conversation, Inventory, Message
from aftercare.services.business import columns
from aftercare.services.fulfillment import FulfillmentService

pytestmark = pytest.mark.integration


def add_messages(service, conversation, rows):
    """Insert immutable log rows with deliberate sort ties, without running an Agent."""
    with service.sessions.begin() as session:
        for suffix, revision, seconds in rows:
            session.add(
                Message(
                    id=f"{conversation}-{suffix}",
                    conversation_id=conversation,
                    client_message_id=suffix,
                    role="assistant" if suffix.endswith("0") else "user",
                    text="完整消息 " + suffix,
                    revision=revision,
                    created_at=service.clock() + timedelta(seconds=seconds),
                )
            )
        row = session.get(Conversation, conversation)
        row.revision = max([row.revision, *[revision for _, revision, _ in rows]])


def message_reference(service, conversation):
    with service.sessions() as session:
        return [
            columns(row)
            for row in session.scalars(
                select(Message)
                .where(Message.conversation_id == conversation)
                .order_by(Message.revision, Message.created_at, Message.id)
            )
        ]


def page(client, path, **params):
    response = client.get(path, params=params)
    assert response.status_code == 200, response.text
    result = response.json()
    assert set(result) == {"items", "next_cursor"}
    return result


def forbidden_full_load(*args, **kwargs):
    raise AssertionError("PAGED_READ_MUST_NOT_LOAD_A_FULL_SNAPSHOT_OR_HISTORY")


def test_message_empty_page_and_legacy_snapshot_shape(api, service):
    conversation = service.create_conversation("alice")
    client = customer(api)
    before = client.get(f"/api/conversations/{conversation}").json()
    assert before["messages"] == []
    assert page(client, f"/api/conversations/{conversation}/messages") == {
        "items": [],
        "next_cursor": None,
    }
    assert client.get(f"/api/conversations/{conversation}").json() == before
    assert "next_cursor" not in before and "messages_next_cursor" not in before


def test_message_default_50_max_100_and_complete_row_fields(api, service):
    conversation = service.create_conversation("alice")
    add_messages(service, conversation, [(f"m{i:03}", 1, 0) for i in range(101)])
    client = customer(api)
    reference = client.get(f"/api/conversations/{conversation}").json()["messages"]
    path = f"/api/conversations/{conversation}/messages"
    first = page(client, path)
    assert first["items"] == reference[:50]
    assert first["next_cursor"] == reference[49]["id"]
    second = page(client, path, after=first["next_cursor"])
    assert second["items"] == reference[50:100]
    third = page(client, path, after=second["next_cursor"])
    assert third == {"items": reference[100:], "next_cursor": None}
    maximum = page(client, path, limit=100)
    assert maximum["items"] == reference[:100]
    assert maximum["next_cursor"] == reference[99]["id"]
    assert set(first["items"][0]) == {
        "id",
        "conversation_id",
        "client_message_id",
        "role",
        "text",
        "revision",
        "created_at",
    }


def test_message_keyset_uses_full_sort_tuple_and_never_calls_locked_snapshot(
    api, service, monkeypatch
):
    conversation = service.create_conversation("alice")
    add_messages(
        service, conversation, [("a", 2, 0), ("z", 1, 1), ("c", 1, 0), ("b", 1, 0), ("d", 1, 0)]
    )
    reference = message_reference(service, conversation)
    client = customer(api)
    monkeypatch.setattr(service, "snapshot", forbidden_full_load)
    monkeypatch.setattr(service, "_conversation", forbidden_full_load)
    path, after, collected = f"/api/conversations/{conversation}/messages", "", []
    for _ in range(4):
        result = page(client, path, limit=2, after=after)
        collected.extend(result["items"])
        if result["next_cursor"] is None:
            break
        after = result["next_cursor"]
    assert collected == reference
    assert len({row["id"] for row in collected}) == len(reference)
    assert page(client, path, after=reference[-1]["id"]) == {"items": [], "next_cursor": None}


@pytest.mark.parametrize(
    "anchor_scope", ["missing", "same_customer_other_conversation", "other_customer"]
)
def test_message_anchor_must_exist_in_authorized_conversation(api, service, anchor_scope):
    conversation = service.create_conversation("alice")
    add_messages(service, conversation, [("a", 1, 0)])
    anchor = "missing-message"
    if anchor_scope != "missing":
        other = service.create_conversation("bob" if anchor_scope == "other_customer" else "alice")
        add_messages(service, other, [("anchor", 1, 0)])
        anchor = other + "-anchor"
    client = customer(api)
    error(
        client.get(f"/api/conversations/{conversation}/messages", params={"after": anchor}),
        400,
        "INVALID_CURSOR",
    )


def test_message_ownership_is_checked_before_anchor_and_headers_cannot_override(api, service):
    conversation = service.create_conversation("alice")
    add_messages(service, conversation, [("private", 1, 0)])
    bob = customer(api, "bob")
    for identity in (conversation, "missing-conversation"):
        error(
            bob.get(
                f"/api/conversations/{identity}/messages",
                params={"after": "missing"},
                headers={"X-Customer-Id": "alice", "X-Role": "support"},
            ),
            404,
            "NOT_FOUND",
        )


def test_message_requires_customer_session_and_keeps_old_staff_boundary(api, service):
    conversation = service.create_conversation("alice")
    path = f"/api/conversations/{conversation}/messages"
    error(api.client().get(path), 401, "DEMO_SESSION_REQUIRED")
    error(staff(api).get(path), 401, "DEMO_SESSION_REQUIRED")


@pytest.mark.parametrize(
    "params", [{"limit": 0}, {"limit": 101}, {"limit": "bad"}, {"after": "x" * 101}]
)
def test_message_invalid_page_parameters(api, service, params):
    conversation = service.create_conversation("alice")
    error(
        customer(api).get(f"/api/conversations/{conversation}/messages", params=params),
        422,
        "INVALID_INPUT",
    )


def test_message_inserts_before_cursor_do_not_shift_following_pages(api, service):
    conversation = service.create_conversation("alice")
    add_messages(service, conversation, [("20", 1, 0), ("40", 1, 0), ("60", 2, 0)])
    client, path = customer(api), f"/api/conversations/{conversation}/messages"
    first = page(client, path, limit=2)
    assert [row["id"] for row in first["items"]] == [conversation + "-20", conversation + "-40"]
    add_messages(service, conversation, [("10", 1, 0), ("50", 1, 0), ("30", 3, 0)])
    following = page(client, path, limit=10, after=first["next_cursor"])
    assert [row["id"] for row in following["items"]] == [
        conversation + "-50",
        conversation + "-60",
        conversation + "-30",
    ]
    assert following["next_cursor"] is None
    # Pages traverse the immutable sort position, not a frozen snapshot across requests.
    assert conversation + "-10" not in {row["id"] for row in following["items"]}


def test_events_default_shape_and_paged_rows_match_complete_legacy_history(
    api, service, monkeypatch
):
    application_id = prepared(service)["application_id"]
    support, owner = staff(api), customer(api)
    success(post(support, application_id, "review", 1, decision="approve"), "AWAITING_RETURN")
    path = f"/api/applications/{application_id}/events"
    expected = FulfillmentService(service.sessions).get(
        # The HTTP route still supplies the session identity; this is only the reference read.
        Actor("alice", "customer", "test_fixture"),
        application_id,
    )["history"]
    assert owner.get(path).json() == {"items": expected}
    monkeypatch.setattr(FulfillmentService, "get", forbidden_full_load)
    first = page(owner, path, limit=1)
    assert first == {"items": expected[:1], "next_cursor": 1}
    assert page(owner, path, limit=1, after=first["next_cursor"]) == {
        "items": expected[1:],
        "next_cursor": None,
    }
    assert page(owner, path, limit=1, after=2) == {"items": [], "next_cursor": None}


def test_events_append_between_pages_is_read_by_version(api, service):
    application_id = prepared(service)["application_id"]
    support, owner = staff(api), customer(api)
    success(post(support, application_id, "review", 1, decision="approve"), "AWAITING_RETURN")
    path = f"/api/applications/{application_id}/events"
    first = page(owner, path, limit=1)
    success(
        post(
            owner,
            application_id,
            "return-shipment",
            2,
            carrier="test-carrier",
            tracking_number="new-return",
        ),
        "RETURN_IN_TRANSIT",
    )
    following = page(owner, path, limit=2, after=first["next_cursor"])
    assert [row["to_version"] for row in first["items"] + following["items"]] == [1, 2, 3]
    assert following["next_cursor"] is None


@pytest.mark.parametrize("after", [0, 1])
def test_events_cursor_requires_explicit_pagination(api, service, after):
    application_id = prepared(service)["application_id"]
    error(
        customer(api).get(f"/api/applications/{application_id}/events", params={"after": after}),
        400,
        "PAGINATION_LIMIT_REQUIRED",
    )


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": 101},
        {"limit": "bad"},
        {"limit": 1, "after": -1},
        {"limit": 1, "after": "bad"},
        {"limit": 1, "after": 2**31},
    ],
)
def test_events_invalid_page_parameters(api, service, params):
    application_id = prepared(service)["application_id"]
    error(
        customer(api).get(f"/api/applications/{application_id}/events", params=params),
        422,
        "INVALID_INPUT",
    )


def test_events_pagination_keeps_customer_ownership_and_staff_read_access(api, service):
    application_id = prepared(service)["application_id"]
    path = f"/api/applications/{application_id}/events"
    error(api.client().get(path, params={"limit": 1}), 401)
    error(
        customer(api, "bob").get(
            path,
            params={"limit": 1, "after": 0},
            headers={"X-Customer-Id": "alice", "X-Role": "support"},
        ),
        404,
        "NOT_FOUND",
    )
    expected = page(customer(api), path, limit=1)
    for role in ("support", "warehouse"):
        assert page(staff(api, role), path, limit=1) == expected


@pytest.mark.parametrize(
    "change,code",
    [({"active": False}, "ACCOUNT_DISABLED"), ({"role": "warehouse"}, "SESSION_EXPIRED")],
)
def test_events_page_rechecks_current_staff_account_and_session(api, service, change, code):
    application_id = prepared(service)["application_id"]
    client = staff(api)
    path = f"/api/applications/{application_id}/events"
    assert page(client, path, limit=1)["items"]
    api.auth.update_account(api.accounts["support"]["id"], **change)
    error(client.get(path, params={"limit": 1}), 401, code)
    error(client.get("/api/applications", params={"limit": 1}), 401, code)


def test_events_return_application_retains_existing_domain_error(api, service):
    application_id = prepared(service, kind="return")["application_id"]
    client = customer(api)
    path = f"/api/applications/{application_id}/events"
    error(client.get(path), 409, "UNSUPPORTED_APPLICATION")
    error(client.get(path, params={"limit": 1}), 409, "UNSUPPORTED_APPLICATION")


def test_application_existing_cursor_shape_and_owner_filter_are_unchanged(api, service):
    with service.sessions.begin() as session:
        session.get(Inventory, "tee-white-L").available = 2
    alice_id = prepared(service)["application_id"]
    bob_id = prepared(service, owner="bob", order="S2001")["application_id"]
    prepared(service, kind="return")
    support = staff(api)
    default = page(support, "/api/applications")
    assert [row["application_id"] for row in default["items"]] == sorted([alice_id, bob_id])
    assert default["next_cursor"] is None
    first = page(support, "/api/applications", limit=1)
    second = page(support, "/api/applications", limit=1, after=first["next_cursor"])
    assert first["items"] + second["items"] == default["items"]
    assert second["next_cursor"] is None
    assert set(default["items"][0]) == {
        "application_id",
        "order_id",
        "quantity",
        "target_variant",
        "status",
        "version",
    }
    bob = page(customer(api, "bob"), "/api/applications", limit=1)
    assert [row["application_id"] for row in bob["items"]] == [bob_id]
    assert bob["next_cursor"] is None
