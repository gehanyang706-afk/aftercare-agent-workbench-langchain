"""Prune the baseline lock using its installed, version-matched package metadata.

This does not resolve or upgrade packages. All surviving pins and artifact hashes
come verbatim from the baseline lock. Evaluate Darwin/Linux separately so Darwin
only roots (torch/transformers) do not introduce Linux CUDA dependencies.
"""

import argparse
import json
import re
import tomllib
from importlib import metadata
from pathlib import Path

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def prune(source, project):
    blocks = re.split(r"(?m)(?=^[a-zA-Z0-9][a-zA-Z0-9_.-]*==)", source)[1:]
    pins = {}
    for block in blocks:
        match = re.match(r"([^=]+)==([^\s\\]+)", block)
        pins[canonicalize_name(match[1])] = (match[2], block)
    roots = (
        project["project"]["dependencies"]
        + project["project"]["optional-dependencies"]["dev"]
        + project["build-system"]["requires"]
    )
    needed, opaque = set(), set()
    for platform, system in (("darwin", "Darwin"), ("linux", "Linux")):
        env = dict(default_environment(), sys_platform=platform, platform_system=system)
        queue = [Requirement(r) for r in roots]
        seen = set()
        while queue:
            req = queue.pop()
            if req.marker and not req.marker.evaluate(dict(env, extra="")):
                continue
            name = canonicalize_name(req.name)
            key = (name, tuple(sorted(req.extras)))
            if key in seen:
                continue
            seen.add(key)
            needed.add(name)
            if name not in pins:
                raise ValueError("DEPENDENCY_NOT_IN_BASELINE_LOCK:" + name)
            try:
                dist = metadata.distribution(name)
            except metadata.PackageNotFoundError:
                opaque.add(name)
                continue
            if dist.version != pins[name][0] or not req.specifier.contains(dist.version):
                raise ValueError("INSTALLED_VERSION_NOT_BASELINE:" + name)
            for spec in dist.requires or []:
                child = Requirement(spec)
                if not child.marker or any(
                    child.marker.evaluate(dict(env, extra=extra)) for extra in req.extras | {""}
                ):
                    # This dependency marker was evaluated in its parent's extras.
                    child.marker = None
                    queue.append(child)
    if opaque:
        raise ValueError("MISSING_METADATA_CANNOT_PRUNE:" + ",".join(sorted(opaque)))
    removed = sorted(set(pins) - needed)
    result = (
        "# Baseline pins/hashes pruned offline; no version or artifact changes.\n"
        "# Source: Git baseline 5b435ead0c3b9f998c89d9d31f510ec29ad77884.\n"
        "# Reproduce using scripts/migration/prune_lock.py and baseline metadata.\n\n"
    )
    for name, (_, block) in pins.items():
        if name not in needed:
            continue
        # Dependency-origin comments from the old resolution may name removed roots.
        lines = [line for line in block.splitlines() if not line.lstrip().startswith("#")]
        result += "\n".join(lines).rstrip() + "\n\n"
    return result.rstrip() + "\n", {
        "baseline_packages": len(pins),
        "retained": len(needed),
        "removed": removed,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-lock", type=Path, required=True)
    parser.add_argument("--project", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output, report = prune(args.baseline_lock.read_text(), tomllib.loads(args.project.read_text()))
    args.output.write_text(output)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
