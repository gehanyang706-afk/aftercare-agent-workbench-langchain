"""Staff credentials, revocation and transaction guards against the dedicated local DB."""

import hashlib
from datetime import timedelta

import pytest
from sqlalchemy import MetaData, func, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import sessionmaker
from test_fulfillment_migration import legacy_schema as legacy_schema
from test_fulfillment_migration import migration, snapshot_rows, upgrade

from aftercare.domain.contracts import BusinessError, utc_now
from aftercare.domain.fulfillment import Actor, FulfillmentCommandInput
from aftercare.repositories.models import StaffAccount, StaffSession
from aftercare.services.fulfillment import FulfillmentService
from aftercare.services.staff_auth import StaffAuthService, hash_password, verify_password

pytestmark = pytest.mark.integration
PASSWORD = "Staff-Test-Password-123!"


@pytest.fixture
def staff_auth(service):
    # Never inherit the business fixture's historical deterministic clock for sessions.
    return StaffAuthService(service.sessions)


def fails(code, call):
    with pytest.raises(BusinessError) as error:
        call()
    assert error.value.code == code


def account(auth, username="support-1", role="support"):
    return auth.create_account(username, PASSWORD, role)


def test_password_hashes_are_salted_and_corrupt_cost_parameters_are_not_trusted():
    first, second = hash_password(PASSWORD), hash_password(PASSWORD)
    assert first != second and PASSWORD not in first
    assert verify_password(PASSWORD, first)
    assert not verify_password("wrong-password", first)
    assert not verify_password(PASSWORD, "malformed")
    assert not verify_password(PASSWORD, first.replace("$16384$", "$1073741824$"))
    assert not verify_password(PASSWORD, first.replace("$5$", "$0$"))


def test_local_account_provisioning_returns_no_credentials_and_normalizes_username(staff_auth):
    created = staff_auth.create_account(" Support-1 ", PASSWORD, "support")
    assert set(created) == {"id", "username", "role", "active", "auth_version"}
    assert created["username"] == "support-1" and created["auth_version"] == 1
    fails("STAFF_EXISTS", lambda: staff_auth.create_account("SUPPORT-1", PASSWORD, "support"))
    with staff_auth.sessions() as s:
        stored = s.get(StaffAccount, created["id"])
        assert stored.password_hash != PASSWORD and verify_password(PASSWORD, stored.password_hash)
        assert s.scalar(select(func.count()).select_from(StaffAccount)) == 1


@pytest.mark.parametrize(
    "username,password,role",
    [
        ("invalid user", PASSWORD, "support"),
        ("", PASSWORD, "support"),
        ("support", "too-short", "support"),
        ("support", " " * 12, "support"),
        ("support", "x" * 129, "support"),
        ("support", PASSWORD, "customer"),
        ("support", PASSWORD, ["support"]),
    ],
)
def test_invalid_account_inputs_are_rejected_without_account(staff_auth, username, password, role):
    fails("STAFF_INPUT_INVALID", lambda: staff_auth.create_account(username, password, role))
    with staff_auth.sessions() as s:
        assert s.scalar(select(func.count()).select_from(StaffAccount)) == 0


def test_real_credentials_mint_separate_random_sessions_and_only_store_token_hash(staff_auth):
    created = account(staff_auth)
    first, second = staff_auth.login("SUPPORT-1", PASSWORD), staff_auth.login("support-1", PASSWORD)
    assert first["token"] != second["token"]
    assert first["csrf_token"] != second["csrf_token"]
    assert first["expires_at"] > utc_now()
    result = staff_auth.authenticate(first["token"])
    assert result["actor"].actor_id == created["id"]
    assert result["actor"].role == "support" and result["actor"].source == "server_session"
    assert "token" not in result and "password_hash" not in result
    assert result["csrf_token"] == first["csrf_token"]
    assert result["session_hash"] == hashlib.sha256(first["token"].encode()).hexdigest()
    with staff_auth.sessions() as s:
        stored = s.get(StaffSession, result["session_hash"])
        assert stored.staff_id == created["id"] and stored.auth_version == 1
        assert stored.token_hash != first["token"] and stored.revoked_at is None
    staff_auth.logout(first["token"])
    staff_auth.logout(first["token"])
    fails("SESSION_EXPIRED", lambda: staff_auth.authenticate(first["token"]))
    assert staff_auth.authenticate(second["token"])["actor"].actor_id == created["id"]


def test_unknown_username_and_wrong_password_have_same_error_without_session(staff_auth):
    account(staff_auth)
    fails("AUTH_FAILED", lambda: staff_auth.login("does-not-exist", PASSWORD))
    fails("AUTH_FAILED", lambda: staff_auth.login("support-1", "incorrect-password"))
    fails("AUTH_FAILED", lambda: staff_auth.login("invalid user", PASSWORD))
    with staff_auth.sessions() as s:
        assert s.scalar(select(func.count()).select_from(StaffSession)) == 0


def test_missing_unknown_and_expired_sessions_are_rejected(staff_auth):
    account(staff_auth)
    fails("SESSION_REQUIRED", lambda: staff_auth.authenticate(None))
    fails("SESSION_EXPIRED", lambda: staff_auth.authenticate("unknown-session"))
    signed = staff_auth.login("support-1", PASSWORD)
    digest = hashlib.sha256(signed["token"].encode()).hexdigest()
    with staff_auth.sessions.begin() as s:
        s.get(StaffSession, digest).expires_at = utc_now() - timedelta(seconds=1)
    fails("SESSION_EXPIRED", lambda: staff_auth.authenticate(signed["token"]))


def test_role_change_invalidates_old_session_and_new_login_reads_current_role(staff_auth):
    created = account(staff_auth)
    signed = staff_auth.login("support-1", PASSWORD)
    changed = staff_auth.update_account(created["id"], role="warehouse")
    assert changed["auth_version"] == 2 and changed["role"] == "warehouse"
    fails("SESSION_EXPIRED", lambda: staff_auth.authenticate(signed["token"]))
    fresh = staff_auth.login("support-1", PASSWORD)
    assert staff_auth.authenticate(fresh["token"])["actor"].role == "warehouse"


def test_account_disable_and_reenable_do_not_revive_old_session(staff_auth):
    created = account(staff_auth)
    signed = staff_auth.login("support-1", PASSWORD)
    staff_auth.update_account(created["id"], active=False)
    fails("ACCOUNT_DISABLED", lambda: staff_auth.authenticate(signed["token"]))
    fails("ACCOUNT_DISABLED", lambda: staff_auth.login("support-1", PASSWORD))
    staff_auth.update_account(created["id"], active=True)
    fails("SESSION_EXPIRED", lambda: staff_auth.authenticate(signed["token"]))
    assert staff_auth.login("support-1", PASSWORD)["actor"].actor_id == created["id"]


def test_password_rotation_and_revoke_all_invalidate_existing_sessions(staff_auth):
    created = account(staff_auth)
    signed = staff_auth.login("support-1", PASSWORD)
    new_password = "New-Staff-Password-456!"
    staff_auth.update_account(created["id"], password=new_password)
    fails("SESSION_EXPIRED", lambda: staff_auth.authenticate(signed["token"]))
    fails("AUTH_FAILED", lambda: staff_auth.login("support-1", PASSWORD))
    first, second = (
        staff_auth.login("support-1", new_password),
        staff_auth.login("support-1", new_password),
    )
    staff_auth.revoke_all(created["id"])
    for current in (first, second):
        fails("SESSION_EXPIRED", lambda current=current: staff_auth.authenticate(current["token"]))
    assert staff_auth.login("support-1", new_password)["actor"].actor_id == created["id"]


def test_guard_refreshes_preloaded_session_after_logout(staff_auth):
    account(staff_auth)
    signed = staff_auth.login("support-1", PASSWORD)
    token_hash = hashlib.sha256(signed["token"].encode()).hexdigest()
    with staff_auth.sessions.begin() as s:
        old = s.get(StaffSession, token_hash)
        assert old.revoked_at is None
        staff_auth.logout(signed["token"])
        fails("SESSION_EXPIRED", lambda: staff_auth.guard(s, signed["token"]))


def test_authentication_reads_active_flag_and_role_from_server_records(staff_auth):
    created = account(staff_auth)
    signed = staff_auth.login("support-1", PASSWORD)
    with staff_auth.sessions.begin() as s:
        s.get(StaffAccount, created["id"]).role = "warehouse"
    assert staff_auth.authenticate(signed["token"])["actor"].role == "warehouse"
    with staff_auth.sessions.begin() as s:
        s.get(StaffAccount, created["id"]).active = False
    fails("ACCOUNT_DISABLED", lambda: staff_auth.authenticate(signed["token"]))


@pytest.mark.parametrize("target", ["account", "session"])
def test_guard_holds_revocation_locks_until_caller_transaction_ends(staff_auth, target):
    created = account(staff_auth)
    signed = staff_auth.login("support-1", PASSWORD)
    token_hash = hashlib.sha256(signed["token"].encode()).hexdigest()
    with staff_auth.sessions.begin() as guarded:
        staff_auth.guard(guarded, signed["token"])
        with pytest.raises(DBAPIError) as error:
            with staff_auth.sessions.begin() as contender:
                contender.execute(text("SET LOCAL lock_timeout='100ms'"))
                if target == "account":
                    contender.execute(
                        update(StaffAccount)
                        .where(StaffAccount.id == created["id"])
                        .values(auth_version=StaffAccount.auth_version + 1)
                    )
                else:
                    contender.execute(
                        update(StaffSession)
                        .where(StaffSession.token_hash == token_hash)
                        .values(revoked_at=utc_now())
                    )
        assert error.value.orig.sqlstate == "55P03"
    if target == "account":
        staff_auth.revoke_all(created["id"])
    else:
        staff_auth.logout(signed["token"])
    fails("SESSION_EXPIRED", lambda: staff_auth.authenticate(signed["token"]))


@pytest.mark.parametrize("values", [{"role": "admin"}, {"auth_version": 0}])
def test_staff_identity_constraints_reject_invalid_persisted_state(staff_auth, values):
    created = account(staff_auth)
    with pytest.raises(IntegrityError):
        with staff_auth.sessions.begin() as s:
            s.execute(update(StaffAccount).where(StaffAccount.id == created["id"]).values(**values))


def test_incremental_staff_auth_migration_preserves_b1_facts_and_rejects_downgrade(legacy_schema):
    connection, _old_tables = legacy_schema
    upgrade(connection, "0007_exchange_fulfillment")
    bound_sessions = sessionmaker(
        connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
    )
    result = FulfillmentService(bound_sessions).execute(
        Actor("pre-0008-support", "support", "test_fixture"),
        "app-EXCHANGE_REQUESTED",
        FulfillmentCommandInput(
            command="approve",
            expected_version=1,
            idempotency_key="preserve-before-staff-auth",
            data={"note": "Preserve B1 command and version-two audit"},
        ),
    )
    assert result["status"] == "AWAITING_RETURN" and result["version"] == 2
    metadata = MetaData()
    metadata.reflect(bind=connection)
    before = snapshot_rows(connection, metadata.tables)
    assert len(before["fulfillment_commands"]) == 1
    assert any(row["to_version"] == 2 for row in before["fulfillment_audits"])

    upgrade(connection, "0008_staff_auth")
    assert snapshot_rows(connection, metadata.tables) == before
    assert connection.scalar(text("SELECT count(*) FROM staff_accounts")) == 0
    assert connection.scalar(text("SELECT count(*) FROM staff_sessions")) == 0
    with pytest.raises(RuntimeError, match="STAFF_AUTH_DOWNGRADE_FORBIDDEN"):
        migration("0008_staff_auth").downgrade()
    assert snapshot_rows(connection, metadata.tables) == before
