"""Cached Docker reuse cannot hide lock drift or silently delete dependencies."""

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from packaging.markers import default_environment

from scripts import backend_runtime_lock as lock

HASH = " --hash=sha256:" + "a" * 64


def environment(system):
    return dict(default_environment(), sys_platform=system)


def test_native_linux_and_darwin_markers_match_the_actual_frozen_lock():
    source = (Path(__file__).resolve().parents[2] / "requirements.lock").read_text()
    linux = lock.parse_lock(source, environment("linux"))
    darwin = lock.parse_lock(source, environment("darwin"))
    assert len(linux) == 87 and len(darwin) == 89
    assert set(darwin) - set(linux) == {"torch", "transformers"}
    assert linux["packaging"] == "26.3" and linux["setuptools"] == "82.0.1"


def test_multiline_hashes_and_native_marker_evaluation_do_not_accept_inactive_pins():
    source = (
        "# Source lock\nFoo_Bar==1.2 \\\n    --hash=sha256:"
        + "a" * 64
        + " \\\n    --hash=sha256:"
        + "b" * 64
        + '\nmac-only==2; sys_platform == "darwin"'
        + HASH
        + "\n"
    )
    assert lock.parse_lock(source, environment("linux")) == {"foo-bar": "1.2"}


@pytest.mark.parametrize(
    "source,code",
    [
        ("# empty\n", "LOCK_EMPTY"),
        ("thing==1 \\", "LOCK_UNFINISHED_CONTINUATION"),
        ("thing==1", "LOCK_SHA256_REQUIRED"),
        ("thing==1 --hash=md5:" + "a" * 32, "LOCK_SHA256_REQUIRED"),
        ("thing==1 --hash=sha256:" + "a" * 63, "LOCK_SHA256_REQUIRED"),
        ("thing>=1" + HASH, "LOCK_EXACT_PIN_REQUIRED"),
        ("thing==1.*" + HASH, "LOCK_EXACT_PIN_REQUIRED"),
        ("thing==1,!=1.2" + HASH, "LOCK_EXACT_PIN_REQUIRED"),
        ("thing[feature]==1" + HASH, "LOCK_EXACT_PIN_REQUIRED"),
        ("thing @ https://invalid.example/thing.whl" + HASH, "LOCK_EXACT_PIN_REQUIRED"),
        ("--index-url https://invalid.example" + HASH, "LOCK_REQUIREMENT_INVALID"),
        ("thing==1 --trusted-host invalid.example" + HASH, "LOCK_REQUIREMENT_INVALID"),
        ("Foo_Bar==1" + HASH + "\nfoo-bar==1" + HASH, "LOCK_DUPLICATE_NAME"),
        ('thing==1;sys_platform=="darwin"' + HASH, "LOCK_NO_NATIVE_PACKAGES"),
    ],
)
def test_ambiguous_unpinned_or_unhashed_lock_entries_fail_closed(source, code):
    with pytest.raises(lock.LockError, match=code):
        lock.parse_lock(source, environment("linux"))


@pytest.mark.parametrize("installed", [{"extra": "2"}, {"thing": "0.9", "extra": "2"}])
def test_missing_or_mismatched_locked_packages_stop_before_any_prune(installed):
    def forbidden(*args, **kwargs):
        pytest.fail("A lock mismatch must never invoke pip")

    result = lock.check_runtime(
        {"thing": "1"}, prune_extras=True, inventory=lambda: installed, run=forbidden
    )
    assert result["status"] == "FAILED" and result["error_code"] == "LOCKED_VERSIONS_DIFFER"
    assert result["pruned"] == [] and not result["locked_precheck_passed"]


def test_default_check_is_read_only_and_refuses_extras():
    def forbidden(*args, **kwargs):
        pytest.fail("Default verification must not mutate the runtime")

    result = lock.check_runtime(
        {"thing": "1"}, inventory=lambda: {"thing": "1", "extra": "2"}, run=forbidden
    )
    assert result["status"] == "FAILED" and result["error_code"] == "UNLOCKED_PACKAGES_PRESENT"
    assert result["extras"] == {"extra": "2"} and result["locked_precheck_passed"]


def test_explicit_prune_preserves_locked_dependencies_and_project_then_rechecks():
    inventories = iter(
        [
            {"thing": "1", "old-b": "2", "old-a": "3", lock.PROJECT: "0.1.0"},
            {"thing": "1", lock.PROJECT: "0.1.0"},
        ]
    )
    calls = []
    result = lock.check_runtime(
        {"thing": "1"},
        prune_extras=True,
        require_project=True,
        inventory=lambda: next(inventories),
        run=lambda argv, **options: calls.append((argv, options)),
    )
    assert calls == [
        ([sys.executable, "-m", "pip", "uninstall", "--yes", "old-a", "old-b"], {"check": True})
    ]
    assert result["status"] == "PASSED" and result["pruned"] == ["old-a", "old-b"]
    assert result["extras"] == {} and result["project_version"] == "0.1.0"


@pytest.mark.parametrize("after", [{}, {"thing": "2"}, {"thing": "1", "extra": "2"}])
def test_post_prune_missing_drift_or_surviving_extra_is_not_success(after):
    inventories = iter([{"thing": "1", "extra": "2"}, after])
    result = lock.check_runtime(
        {"thing": "1"},
        prune_extras=True,
        inventory=lambda: next(inventories),
        run=lambda *args, **kwargs: None,
    )
    assert result["status"] == "FAILED"
    assert result["error_code"] == "POST_PRUNE_INVENTORY_DIFFERS"


def test_final_check_can_require_project_metadata():
    result = lock.check_runtime(
        {"thing": "1"}, require_project=True, inventory=lambda: {"thing": "1"}
    )
    assert result["error_code"] == "PROJECT_DISTRIBUTION_MISSING"


def test_required_project_must_also_survive_pruning():
    inventories = iter([{"thing": "1", "extra": "2", lock.PROJECT: "0.1.0"}, {"thing": "1"}])
    result = lock.check_runtime(
        {"thing": "1"},
        prune_extras=True,
        require_project=True,
        inventory=lambda: next(inventories),
        run=lambda *args, **kwargs: None,
    )
    assert result["status"] == "FAILED"
    assert result["error_code"] == "POST_PRUNE_INVENTORY_DIFFERS"


def test_matching_runtime_never_spawns_pip_even_if_prune_was_requested():
    def forbidden(*args, **kwargs):
        pytest.fail("Already exact runtime should not spawn an uninstall")

    result = lock.check_runtime(
        {"thing": "1"}, prune_extras=True, inventory=lambda: {"thing": "1"}, run=forbidden
    )
    assert result["status"] == "PASSED" and not result["pruned"]


def test_inventory_normalizes_names_and_refuses_conflicting_duplicate_metadata():
    first = SimpleNamespace(metadata={"Name": "Foo_Bar"}, version="1")
    same = SimpleNamespace(metadata={"Name": "foo-bar"}, version="1")
    conflict = SimpleNamespace(metadata={"Name": "foo-bar"}, version="2")
    assert lock.installed_versions([first, same]) == {"foo-bar": "1"}
    with pytest.raises(lock.LockError, match="INSTALLED_METADATA_CONFLICT"):
        lock.installed_versions([first, conflict])


def test_cli_does_not_claim_installed_metadata_revalidates_original_wheel_hashes(
    tmp_path, monkeypatch, capsys
):
    path = tmp_path / "lock.txt"
    path.write_text("thing==1" + HASH)
    monkeypatch.setattr(lock, "installed_versions", lambda: {"thing": "1"})
    assert lock.main(["--lock", str(path)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "PASSED"
    assert "wheel hashes not revalidated" in report["verification"]
    assert len(report["lock_sha256"]) == 64


def test_failed_pip_uninstall_makes_cli_fail_instead_of_claiming_exact_environment(
    tmp_path, monkeypatch, capsys
):
    path = tmp_path / "lock.txt"
    path.write_text("thing==1" + HASH)
    monkeypatch.setattr(lock, "installed_versions", lambda: {"thing": "1", "extra": "2"})

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args[0])

    monkeypatch.setattr(lock.subprocess, "run", fail)
    assert lock.main(["--lock", str(path), "--prune-extras"]) == 1
    assert json.loads(capsys.readouterr().out)["error_code"] == "CalledProcessError"
