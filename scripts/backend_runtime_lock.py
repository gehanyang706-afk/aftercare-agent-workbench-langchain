"""Check native-platform lock versions; prune extras only when explicitly asked.

The cached-runtime path checks installed distribution metadata. It neither
downloads dependencies nor claims to revalidate the original wheel hashes.
Missing or mismatched locked packages always stop before any uninstall command.
The current project distribution is retained for the Dockerfile's editable install.
"""

import argparse
import hashlib
import json
import platform
import re
import subprocess
import sys
from importlib import metadata
from pathlib import Path

from packaging.markers import default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import InvalidName, canonicalize_name

PROJECT = "aftercare-agent-workbench"
HASH = re.compile(r"\s+--hash=sha256:[a-f0-9]{64}(?=\s|$)")


class LockError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise LockError(code)


def parse_lock(source, environment=None):
    """Parse this repository's hashed, exact-pin format without accepting options."""
    environment = dict(default_environment() if environment is None else environment, extra="")
    logical, pending = [], ""
    for line in source.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        continued = stripped.endswith("\\")
        pending += " " + (stripped[:-1] if continued else stripped)
        if not continued:
            logical.append(pending.strip())
            pending = ""
    require(not pending, "LOCK_UNFINISHED_CONTINUATION")
    require(bool(logical), "LOCK_EMPTY")
    result, seen = {}, set()
    for entry in logical:
        requirement, hashes = HASH.subn("", entry)
        require(hashes > 0, "LOCK_SHA256_REQUIRED")
        try:
            spec = Requirement(requirement.strip())
        except InvalidRequirement:
            raise LockError("LOCK_REQUIREMENT_INVALID") from None
        pins = list(spec.specifier)
        require(
            spec.url is None
            and not spec.extras
            and len(pins) == 1
            and pins[0].operator == "=="
            and "*" not in pins[0].version,
            "LOCK_EXACT_PIN_REQUIRED",
        )
        name = canonicalize_name(spec.name)
        require(name not in seen, "LOCK_DUPLICATE_NAME")
        seen.add(name)
        if spec.marker is None or spec.marker.evaluate(environment):
            result[name] = pins[0].version
    require(bool(result), "LOCK_NO_NATIVE_PACKAGES")
    return result


def installed_versions(distributions=None):
    """Refuse ambiguous conflicting metadata instead of hiding a duplicate version."""
    result = {}
    for distribution in metadata.distributions() if distributions is None else distributions:
        try:
            name = canonicalize_name(distribution.metadata["Name"], validate=True)
        except (InvalidName, TypeError):
            raise LockError("INSTALLED_DISTRIBUTION_NAME_INVALID") from None
        version = distribution.version
        require(type(version) is str and bool(version), "INSTALLED_VERSION_INVALID")
        require(name not in result or result[name] == version, "INSTALLED_METADATA_CONFLICT")
        result[name] = version
    return result


def inspect_versions(expected, installed):
    return {
        "expected_count": len(expected),
        "installed_count": len(installed),
        "missing": sorted(set(expected) - set(installed)),
        "mismatch": {
            name: {"expected": version, "installed": installed[name]}
            for name, version in sorted(expected.items())
            if name in installed and installed[name] != version
        },
        "extras": {
            name: version
            for name, version in sorted(installed.items())
            if name not in expected and name != PROJECT
        },
        "project_version": installed.get(PROJECT),
    }


def check_runtime(expected, *, prune_extras=False, require_project=False, inventory=None, run=None):
    """Inventory/prune/inventory; never fix a lock mismatch by uninstalling packages."""
    require(type(prune_extras) is bool, "PRUNE_FLAG_INVALID")
    require(type(require_project) is bool, "PROJECT_FLAG_INVALID")
    inventory = installed_versions if inventory is None else inventory
    before = inspect_versions(expected, inventory())
    report = {**before, "pruned": [], "locked_precheck_passed": False}
    if before["missing"] or before["mismatch"]:
        return {**report, "status": "FAILED", "error_code": "LOCKED_VERSIONS_DIFFER"}
    if require_project and before["project_version"] is None:
        return {**report, "status": "FAILED", "error_code": "PROJECT_DISTRIBUTION_MISSING"}
    report["locked_precheck_passed"] = True
    if before["extras"] and not prune_extras:
        return {**report, "status": "FAILED", "error_code": "UNLOCKED_PACKAGES_PRESENT"}
    if before["extras"]:
        run = subprocess.run if run is None else run
        packages = sorted(before["extras"])
        run([sys.executable, "-m", "pip", "uninstall", "--yes", *packages], check=True)
        after = inspect_versions(expected, inventory())
        report.update(after, pruned=packages)
        if (
            after["missing"]
            or after["mismatch"]
            or after["extras"]
            or (require_project and after["project_version"] is None)
        ):
            return {**report, "status": "FAILED", "error_code": "POST_PRUNE_INVENTORY_DIFFERS"}
    return {**report, "status": "PASSED"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, default=Path("requirements.lock"))
    parser.add_argument("--prune-extras", action="store_true")
    parser.add_argument("--require-project", action="store_true")
    args = parser.parse_args(argv)
    try:
        source = args.lock.read_bytes()
        expected = parse_lock(source.decode("utf-8"))
        report = check_runtime(
            expected, prune_extras=args.prune_extras, require_project=args.require_project
        )
        report.update(
            lock_sha256=hashlib.sha256(source).hexdigest(),
            python=platform.python_version(),
            native_platform=sys.platform,
            verification="installed distribution metadata versions; wheel hashes not revalidated",
        )
    except (OSError, UnicodeError, LockError, subprocess.CalledProcessError) as error:
        report = {
            "status": "FAILED",
            "error_code": str(error) if isinstance(error, LockError) else type(error).__name__,
        }
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
