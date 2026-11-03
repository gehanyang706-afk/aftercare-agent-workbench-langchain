"""Local staff credentials and server-side, revocable sessions; no HTTP self-registration.

Scrypt uses a fixed 16 MiB / p=5 profile. Restrict hash concurrency and never trust
cost parameters stored in a hash. Account -> session is the authorization lock order.
"""

import base64
import hashlib
import re
import secrets
from datetime import timedelta
from threading import BoundedSemaphore

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from aftercare.domain.contracts import BusinessError, utc_now
from aftercare.domain.fulfillment import Actor
from aftercare.domain.rules import require
from aftercare.repositories.models import StaffAccount, StaffSession
from aftercare.services.business import new_id

SCRYPT_N, SCRYPT_R, SCRYPT_P = 16384, 8, 5
SCRYPT_MAXMEM = 64 * 1024 * 1024
SESSION_TTL = timedelta(hours=8)
_HASH_SLOTS = BoundedSemaphore(2)
_HASH_PREFIX = f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}"
_DUMMY_HASH = (
    _HASH_PREFIX
    + "$"
    + base64.b64encode(bytes(16)).decode("ascii")
    + "$"
    + base64.b64encode(bytes(32)).decode("ascii")
)


def _password_bytes(password):
    if not isinstance(password, str) or not 1 <= len(password) <= 128:
        return None
    try:
        return password.encode("utf-8")
    except UnicodeEncodeError:
        return None


def _derive(password_bytes, salt):
    with _HASH_SLOTS:
        return hashlib.scrypt(
            password_bytes,
            salt=salt,
            n=SCRYPT_N,
            r=SCRYPT_R,
            p=SCRYPT_P,
            maxmem=SCRYPT_MAXMEM,
            dklen=32,
        )


def hash_password(password):
    encoded = _password_bytes(password)
    require(
        encoded is not None and len(password) >= 12 and not password.isspace(),
        "STAFF_INPUT_INVALID",
    )
    salt = secrets.token_bytes(16)
    derived = _derive(encoded, salt)
    return (
        _HASH_PREFIX
        + "$"
        + base64.b64encode(salt).decode("ascii")
        + "$"
        + base64.b64encode(derived).decode("ascii")
    )


def verify_password(password, password_hash):
    encoded = _password_bytes(password)
    if encoded is None:
        return False
    valid = True
    try:
        if not isinstance(password_hash, str) or len(password_hash) > 200:
            raise ValueError("INVALID_PASSWORD_HASH")
        scheme, n, r, p, salt_string, digest_string = password_hash.split("$")
        if (scheme, n, r, p) != ("scrypt", str(SCRYPT_N), str(SCRYPT_R), str(SCRYPT_P)):
            raise ValueError("INVALID_PASSWORD_HASH")
        salt = base64.b64decode(salt_string, validate=True)
        expected = base64.b64decode(digest_string, validate=True)
        if len(salt) != 16 or len(expected) != 32:
            raise ValueError("INVALID_PASSWORD_HASH")
    except (TypeError, ValueError):
        salt, expected, valid = bytes(16), bytes(32), False
    actual = _derive(encoded, salt)
    return secrets.compare_digest(actual, expected) and valid


def _username(value):
    require(isinstance(value, str), "STAFF_INPUT_INVALID")
    normalized = value.strip().lower()
    require(
        re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,99}", normalized) is not None, "STAFF_INPUT_INVALID"
    )
    return normalized


def _safe_account(account):
    return {
        "id": account.id,
        "username": account.username,
        "role": account.role,
        "active": account.active,
        "auth_version": account.auth_version,
    }


def _session_hash(token):
    require(token is not None and token != "", "SESSION_REQUIRED")
    require(isinstance(token, str) and len(token) <= 512, "SESSION_EXPIRED")
    try:
        encoded = token.encode("utf-8")
    except UnicodeEncodeError:
        raise BusinessError("SESSION_EXPIRED") from None
    return hashlib.sha256(encoded).hexdigest()


def _lock(s, model, identity, *, shared=False):
    return s.scalar(
        select(model)
        .where(model.__mapper__.primary_key[0] == identity)
        .with_for_update(read=shared)
        .execution_options(populate_existing=True)
    )


class StaffAuthService:
    def __init__(self, sessions, clock=utc_now):
        self.sessions, self.clock = sessions, clock

    def create_account(self, username, password, role):
        """Trusted local provisioning only; intentionally has no HTTP endpoint."""
        username = _username(username)
        require(isinstance(role, str) and role in {"support", "warehouse"}, "STAFF_INPUT_INVALID")
        password_hash = hash_password(password)
        try:
            with self.sessions.begin() as s:
                account = StaffAccount(
                    id=new_id("staff"),
                    username=username,
                    password_hash=password_hash,
                    role=role,
                    active=True,
                    auth_version=1,
                    created_at=self.clock(),
                )
                s.add(account)
                s.flush()
                return _safe_account(account)
        except IntegrityError as error:
            if getattr(getattr(error.orig, "diag", None), "constraint_name", "") == (
                "staff_accounts_username_key"
            ):
                raise BusinessError("STAFF_EXISTS") from None
            raise

    def login(self, username, password):
        try:
            username = _username(username)
        except BusinessError:
            verify_password(password, _DUMMY_HASH)
            raise BusinessError("AUTH_FAILED") from None
        with self.sessions() as s:
            account = s.scalar(select(StaffAccount).where(StaffAccount.username == username))
            snapshot = (
                (account.id, account.password_hash, account.auth_version) if account else None
            )
        valid = verify_password(password, snapshot[1] if snapshot else _DUMMY_HASH)
        require(valid and snapshot is not None, "AUTH_FAILED")
        token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        with self.sessions.begin() as s:
            account = _lock(s, StaffAccount, snapshot[0], shared=True)
            require(
                account is not None
                and account.password_hash == snapshot[1]
                and account.auth_version == snapshot[2],
                "AUTH_FAILED",
            )
            require(account.active, "ACCOUNT_DISABLED")
            now = self.clock()
            expires = now + SESSION_TTL
            s.add(
                StaffSession(
                    token_hash=_session_hash(token),
                    staff_id=account.id,
                    csrf_token=csrf,
                    auth_version=account.auth_version,
                    expires_at=expires,
                    created_at=now,
                )
            )
            return {
                "token": token,
                "csrf_token": csrf,
                "actor": Actor(account.id, account.role, "server_session"),
                "expires_at": expires,
                "username": account.username,
            }

    def guard(self, session, token):
        """Hold shared account/session locks until the caller's transaction ends."""
        token_hash = _session_hash(token)
        initial = session.get(StaffSession, token_hash)
        require(initial is not None, "SESSION_EXPIRED")
        staff_id = initial.staff_id
        account = _lock(session, StaffAccount, staff_id, shared=True)
        require(account is not None, "SESSION_EXPIRED")
        current = _lock(session, StaffSession, token_hash, shared=True)
        require(
            current is not None
            and current.staff_id == staff_id
            and current.revoked_at is None
            and current.expires_at > self.clock(),
            "SESSION_EXPIRED",
        )
        require(account.active, "ACCOUNT_DISABLED")
        require(current.auth_version == account.auth_version, "SESSION_EXPIRED")
        return {
            "actor": Actor(account.id, account.role, "server_session"),
            "csrf_token": current.csrf_token,
            "session_hash": current.token_hash,
            "expires_at": current.expires_at,
            "username": account.username,
        }

    def authenticate(self, token):
        with self.sessions.begin() as s:
            return self.guard(s, token)

    def logout(self, token):
        token_hash = _session_hash(token)
        with self.sessions.begin() as s:
            initial = s.get(StaffSession, token_hash)
            if initial is None:
                return
            # Match guard's account -> session order; a guarded write must finish first.
            _lock(s, StaffAccount, initial.staff_id, shared=True)
            current = _lock(s, StaffSession, token_hash)
            if current is not None and current.revoked_at is None:
                current.revoked_at = self.clock()

    def revoke_all(self, staff_id):
        with self.sessions.begin() as s:
            account = _lock(s, StaffAccount, staff_id)
            require(account is not None, "NOT_FOUND")
            account.auth_version += 1
            return _safe_account(account)

    def update_account(self, staff_id, *, role=None, active=None, password=None):
        """Trusted local management; every change invalidates prior sessions."""
        require(any(v is not None for v in (role, active, password)), "STAFF_INPUT_INVALID")
        require(
            role is None or isinstance(role, str) and role in {"support", "warehouse"},
            "STAFF_INPUT_INVALID",
        )
        require(active is None or isinstance(active, bool), "STAFF_INPUT_INVALID")
        new_hash = hash_password(password) if password is not None else None
        with self.sessions.begin() as s:
            account = _lock(s, StaffAccount, staff_id)
            require(account is not None, "NOT_FOUND")
            if role is not None:
                account.role = role
            if active is not None:
                account.active = active
            if new_hash is not None:
                account.password_hash = new_hash
            account.auth_version += 1
            return _safe_account(account)
