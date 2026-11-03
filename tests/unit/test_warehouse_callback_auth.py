"""Callback signing byte contract; HTTP tests cover source/time/window/body limits."""

import hashlib
import hmac

import pytest

from aftercare.domain.warehouse import sign_event, verify_event_signature

SECRET = "C2-test-callback-secret-1234567890123456"
BODY = b'{"event_id":"event-1"}'
STAMP = "1700000000"
GOLDEN = "v1=2b167c3759f99fb2a1ca68f33be30fbf0927d1403fce5ae8eb35cf78be9a39a9"


def test_signing_matches_the_independent_fixed_hmac_vector():
    assert sign_event(SECRET, BODY, STAMP) == GOLDEN
    assert sign_event(SECRET, BODY, int(STAMP)) == GOLDEN
    assert verify_event_signature(SECRET, BODY, STAMP, GOLDEN) is True


def test_signature_covers_the_original_body_bytes_without_json_normalization():
    formatted = b'{ "event_id": "event-1" }'
    assert verify_event_signature(SECRET, formatted, STAMP, GOLDEN) is False
    expected = (
        "v1="
        + hmac.new(SECRET.encode(), STAMP.encode() + b"." + formatted, hashlib.sha256).hexdigest()
    )
    assert sign_event(SECRET, formatted, STAMP) == expected
    assert expected != GOLDEN


@pytest.mark.parametrize(
    "secret,body,stamp",
    [
        ("another-callback-secret-1234567890123456", BODY, STAMP),
        (SECRET, BODY.replace(b"event-1", b"event-2"), STAMP),
        (SECRET, BODY, "1700000001"),
    ],
)
def test_signature_rejects_a_changed_secret_body_or_timestamp(secret, body, stamp):
    assert verify_event_signature(secret, body, stamp, GOLDEN) is False


@pytest.mark.parametrize(
    "signature",
    [
        "",
        "v2=" + GOLDEN[3:],
        GOLDEN[3:],
        "v1=abc",
        "v1=" + "g" * 64,
        "v1=" + "0" * 64,
        GOLDEN + "suffix",
    ],
)
def test_malformed_or_incorrect_signatures_are_not_accepted(signature):
    assert verify_event_signature(SECRET, BODY, STAMP, signature) is False


@pytest.mark.parametrize("secret", ["", "x" * 31, "x" * 257, "x" * 31 + " ", "x" * 31 + "中"])
def test_invalid_callback_secrets_fail_with_a_constant_safe_error(secret):
    with pytest.raises(ValueError, match="WAREHOUSE_CALLBACK_SECRET_REQUIRED") as error:
        sign_event(secret, BODY, STAMP)
    if secret:
        assert secret not in str(error.value)
