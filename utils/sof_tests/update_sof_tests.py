#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Re-imports the SQL-on-FHIR v2 conformance test suite from upstream.

The suite lives in the `tests/` directory of https://github.com/FHIR/sql-on-fhir.js
and is vendored into `pipelines/common/src/test/resources/sql-on-fhir-v2-tests/`,
where `SQLonFHIRv2Test` picks up every `*.json` file automatically.

Files are copied in verbatim at a *pinned* upstream commit recorded in
`upstream.lock`, so re-running this script is reproducible and upstream changes can
never break the build without someone deliberately bumping the pin.

A small number of files need to deviate from upstream because of implementation
constraints. Those are checked in under `overlays/` as complete, human-reviewed
replacements, each guarded by the SHA-256 of the upstream file it replaces. If
upstream edits a file we overlay, the guard fails loudly rather than silently
keeping a stale local deviation. The expectations in an overlay are a human claim
about what the spec requires, which is why they are reviewed data and not generated:
deriving them mechanically would mean re-implementing the very behaviour the
conformance test is meant to check independently.

Example usage:
    # Re-import at the currently pinned commit (the common case).
    python3 utils/sof_tests/update_sof_tests.py

    # Move the pin to a new upstream commit, tag, or branch.
    python3 utils/sof_tests/update_sof_tests.py --ref main

    # Report what would change without touching the working tree.
    python3 utils/sof_tests/update_sof_tests.py --dry-run
"""

import argparse
import hashlib
import io
import json
import logging
import sys
import tarfile
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
LOCK_FILE = SCRIPT_DIR / "upstream.lock"
OVERLAY_DIR = SCRIPT_DIR / "overlays"
TESTS_DIR = (
    REPO_ROOT
    / "pipelines"
    / "common"
    / "src"
    / "test"
    / "resources"
    / "sql-on-fhir-v2-tests"
)
RESULT_FILE = TESTS_DIR.parent / "sql-on-fhir-v2-test-result.json"

GUARD_SUFFIX = ".upstream.sha256"
API_URL = "https://api.github.com/repos/{repo}/commits/{ref}"
TARBALL_URL = "https://codeload.github.com/{repo}/tar.gz/{sha}"
HTTP_TIMEOUT_SECONDS = 60

# Regenerating the conformance scorecard is a separate, deliberate step: it needs a
# working Maven build, and the result file is published to upstream's
# implementations.json, so it should never be refreshed as a silent side effect.
REGENERATE_HINT = """
Next step - regenerate the conformance result file:

  mvn test -pl pipelines/common -am -Dtest=SQLonFHIRv2Test \\
      -Dsurefire.failIfNoSpecifiedTests=false \\
      -Dspotless.apply.skip=true \\
      -Dsofv2.resultFile=%s

Without -Dsofv2.resultFile the report goes to a temp file whose location is
platform dependent - on macOS that is the private per-user directory in $TMPDIR
(/var/folders/.../T/), not /tmp - and has to be copied over by hand.

That file is referenced by upstream's implementations.json, so it should be
refreshed in the same change as any test-suite update.
"""


class UpdateError(Exception):
    """A fatal, human-actionable problem with the re-import."""


def sha256_bytes(payload: bytes) -> str:
    """Returns the hex SHA-256 digest of `payload`."""
    return hashlib.sha256(payload).hexdigest()


def load_lock() -> Dict[str, str]:
    """Reads the pinned upstream coordinates."""
    if not LOCK_FILE.exists():
        raise UpdateError(f"Missing lock file: {LOCK_FILE}")
    with LOCK_FILE.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_lock(lock: Dict[str, str]) -> None:
    """Writes the lock file back with a trailing newline."""
    with LOCK_FILE.open("w", encoding="utf-8") as handle:
        json.dump(lock, handle, indent=2)
        handle.write("\n")


def resolve_ref(repo: str, ref: str) -> str:
    """Resolves a branch, tag, or SHA to a concrete commit SHA."""
    url = API_URL.format(repo=repo, ref=ref)
    logger.info("Resolving %s@%s", repo, ref)
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as response:
        payload = json.load(response)
    sha = payload.get("sha")
    if not sha:
        raise UpdateError(f"Could not resolve ref '{ref}' in {repo}")
    return sha


def fetch_upstream_tests(repo: str, sha: str, subdir: str) -> Dict[str, bytes]:
    """Downloads the pinned tarball and returns `{filename: raw bytes}`.

    Bytes are returned untouched: re-serialising the JSON here would reformat every
    file and bury the real upstream delta in noise.
    """
    url = TARBALL_URL.format(repo=repo, sha=sha)
    logger.info("Downloading %s", url)
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as response:
        blob = response.read()
    logger.info("Fetched %d bytes", len(blob))

    files: Dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            parts = Path(member.name).parts
            # Archive layout is `<repo>-<sha>/<subdir>/<file>.json`.
            if len(parts) != 3 or parts[1] != subdir:
                continue
            name = parts[2]
            if not name.endswith(".json"):
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            files[name] = extracted.read()

    if not files:
        raise UpdateError(f"No {subdir}/*.json files found in {repo}@{sha}")
    return files


def validate_json(files: Dict[str, bytes]) -> None:
    """Fails early if upstream shipped a file we cannot parse."""
    for name, payload in sorted(files.items()):
        try:
            json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise UpdateError(f"Upstream file {name} is not valid JSON: {err}") from err


def discover_overlays() -> Dict[str, Tuple[Path, str]]:
    """Returns `{filename: (overlay path, expected upstream sha256)}`."""
    overlays: Dict[str, Tuple[Path, str]] = {}
    if not OVERLAY_DIR.is_dir():
        return overlays
    for guard in sorted(OVERLAY_DIR.glob(f"*{GUARD_SUFFIX}")):
        name = guard.name[: -len(GUARD_SUFFIX)]
        replacement = OVERLAY_DIR / name
        if not replacement.exists():
            raise UpdateError(
                f"Guard {guard.name} has no matching overlay file {replacement.name}"
            )
        overlays[name] = (replacement, guard.read_text(encoding="utf-8").strip())
    return overlays


def apply_overlays(
    files: Dict[str, bytes], overlays: Dict[str, Tuple[Path, str]]
) -> List[str]:
    """Replaces overlaid files in place, verifying each upstream guard first.

    Raises `UpdateError` without modifying anything if a guard does not match, so a
    stale local deviation can never be applied on top of changed upstream content.
    """
    applied: List[str] = []
    for name, (replacement, expected) in sorted(overlays.items()):
        if name not in files:
            raise UpdateError(
                f"Overlay '{name}' no longer exists upstream. Remove "
                f"{replacement.name} and its guard, or re-point the overlay."
            )
        actual = sha256_bytes(files[name])
        if actual != expected:
            raise UpdateError(
                f"Upstream '{name}' changed since the local overlay was written.\n"
                f"  expected sha256 {expected}\n"
                f"  actual   sha256 {actual}\n"
                f"The overlay may no longer be correct. Review the upstream change, "
                f"update {replacement.relative_to(REPO_ROOT)} to match, then record "
                f"the new hash in {replacement.name}{GUARD_SUFFIX}.\n"
                f"If the underlying constraint is gone, delete both files instead so "
                f"the upstream version is vendored verbatim."
            )
        files[name] = replacement.read_bytes()
        applied.append(name)
    return applied


def summarize(files: Dict[str, bytes]) -> Tuple[List[str], List[str], List[str], int]:
    """Compares the prepared files against what is on disk."""
    existing = {p.name for p in TESTS_DIR.glob("*.json")}
    incoming = set(files)
    added = sorted(incoming - existing)
    removed = sorted(existing - incoming)
    changed: List[str] = []
    unchanged = 0
    for name in sorted(incoming & existing):
        if (TESTS_DIR / name).read_bytes() != files[name]:
            changed.append(name)
        else:
            unchanged += 1
    return added, removed, changed, unchanged


def write_tests(files: Dict[str, bytes], removed: List[str]) -> None:
    """Writes the vendored tree, honouring upstream deletions."""
    TESTS_DIR.mkdir(parents=True, exist_ok=True)
    for name in removed:
        (TESTS_DIR / name).unlink()
    for name, payload in sorted(files.items()):
        (TESTS_DIR / name).write_bytes(payload)


def report(
    added: List[str],
    removed: List[str],
    changed: List[str],
    unchanged: int,
    applied: List[str],
) -> None:
    """Prints the summary a reviewer reads before committing."""

    def show(label: str, names: List[str]) -> None:
        logger.info("  %-9s (%d)%s", label, len(names), ":" if names else "")
        for name in names:
            logger.info("      %s", name)

    show("added", added)
    show("removed", removed)
    show("changed", changed)
    show("overlaid", applied)
    logger.info("  %-9s (%d)", "unchanged", unchanged)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--ref",
        help=(
            "Upstream branch, tag, or SHA to pin to. Resolved to a concrete commit "
            "and written back to upstream.lock. Defaults to the pinned commit."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing anything.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Verify the vendored tree matches the pin and the overlays, writing "
            "nothing. Exits non-zero if it has drifted, e.g. after a hand-edit."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point; returns a process exit code."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        lock = load_lock()
        repo = lock["repo"]
        subdir = lock.get("subdir", "tests")

        if args.ref:
            sha = resolve_ref(repo, args.ref)
            logger.info("Pinning %s -> %s", args.ref, sha)
        else:
            sha = lock["sha"]
            logger.info("Using pinned commit %s (%s)", sha, lock.get("ref", "?"))

        files = fetch_upstream_tests(repo, sha, subdir)
        validate_json(files)
        applied = apply_overlays(files, discover_overlays())

        added, removed, changed, unchanged = summarize(files)
        logger.info("SQL-on-FHIR v2 test suite @ %s:", sha[:12])
        report(added, removed, changed, unchanged, applied)

        if args.check:
            if added or removed or changed:
                logger.error(
                    "\nERROR: the vendored tree has drifted from %s@%s.\n"
                    "Re-run without --check to restore it.",
                    repo,
                    sha[:12],
                )
                return 1
            logger.info("\nVendored tree matches the pin.")
            return 0

        if args.dry_run:
            logger.info("\nDry run: nothing written.")
            return 0

        write_tests(files, removed)
        if args.ref:
            lock["ref"] = args.ref
            lock["sha"] = sha
            lock["retrieved"] = date.today().isoformat()
            write_lock(lock)
            logger.info("Updated %s", LOCK_FILE.relative_to(REPO_ROOT))

        if added or removed or changed:
            logger.info(REGENERATE_HINT, RESULT_FILE)
        else:
            logger.info("\nVendored tree already up to date.")
        return 0
    except UpdateError as err:
        logger.error("ERROR: %s", err)
        return 1
    except (urllib.error.URLError, tarfile.TarError, OSError) as err:
        logger.error("ERROR: could not re-import tests: %s", err)
        return 1


if __name__ == "__main__":
    sys.exit(main())
