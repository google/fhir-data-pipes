# SQL-on-FHIR v2 test-suite vendoring

`update_sof_tests.py` re-imports the SQL-on-FHIR v2 conformance test suite from
the `tests/` directory of
[FHIR/sql-on-fhir.js](https://github.com/FHIR/sql-on-fhir.js) into
`pipelines/common/src/test/resources/sql-on-fhir-v2-tests/`, where
[`SQLonFHIRv2Test`](../../pipelines/common/src/test/java/com/google/fhir/analytics/view/SQLonFHIRv2Test.java)
picks up every `*.json` file automatically.

The import is **pinned**: `upstream.lock` records the exact upstream commit, so
re-running the script is reproducible and an upstream change can never break our
build until someone deliberately bumps the pin.

The script needs only the Python standard library.

## Re-import at the current pin

```bash
python3 utils/sof_tests/update_sof_tests.py
```

This should be a no-op on a clean checkout. Add `--dry-run` to see what would
change without touching the working tree.

## Move to a newer upstream commit

```bash
python3 utils/sof_tests/update_sof_tests.py --ref main   # or a tag, or a SHA
```

The ref is resolved to a concrete commit SHA and written back to
`upstream.lock`. Review the reported added/removed/changed files, then
regenerate the conformance result file (see below).

## Regenerating the conformance result file

`pipelines/common/src/test/resources/sql-on-fhir-v2-test-result.json` is
referenced by upstream's
[`implementations.json`](https://github.com/FHIR/sql-on-fhir.js/blob/main/implementations.json),
so refresh it in the same change as any suite update:

```bash
mvn test -pl pipelines/common -am -Dtest=SQLonFHIRv2Test \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -Dspotless.apply.skip=true \
    -Dsofv2.resultFile=$(pwd)/pipelines/common/src/test/resources/sql-on-fhir-v2-test-result.json
```

`-Dsofv2.resultFile` writes the report straight to its checked-in location.
Without it the report goes to a temp file — the test logs the path — which then
has to be copied over by hand.

`-Dspotless.apply.skip=true` is added because Spotless is bound to the
`compile` phase, so without it a plain `mvn test` reformats every file it
compiles and rewrites license headers with the current year — meaning a command
whose only job is to regenerate a JSON report can leave unrelated copyright-year
edits across the tree.

**Worth knowing on macOS.** The default temp directory is *not* `/tmp`. Java uses
`java.io.tmpdir`, which comes from `$TMPDIR`, and macOS sets that per login
session to a private per-user directory such as
`/var/folders/dw/ffnp052x5y33n83vbz_cp2_m0000gp/T/` (mode `700`, unlike the
shared, world-writable `/tmp`). So on a Mac the file lands somewhere non-obvious,
and since every run creates a new randomly-named file it is easy to lose track of
which one is current. On Linux — including CI — `$TMPDIR` is usually unset and
the file does land in `/tmp`. Passing `-Dsofv2.resultFile` sidesteps the
difference entirely, which is why it is the recommended form above. To list them
on a Mac: `ls -lt "$TMPDIR"sql-on-fhir-v2-test-result-*.json`.

The script deliberately does not run Maven itself — publishing a scorecard should
be an explicit step, not a side effect of syncing files.

## Overlays: deliberate local deviations

A few upstream files cannot be used verbatim because of implementation
constraints. Each such file is checked in under `overlays/` as a **complete
replacement**, paired with a guard holding the SHA-256 of the upstream file it
replaces:

```
overlays/fn_reference_keys.json                    # what we vendor instead
overlays/fn_reference_keys.json.upstream.sha256    # hash of upstream's version
```

On every run the script hashes the upstream file and compares it to the guard:

- **match** — the overlay is copied over the fetched file;
- **mismatch** — the run **fails and writes nothing**, because upstream changed
  a file we deviate from and the local replacement may no longer be correct.

### Why overlays are reviewed files, not a generated transform

The current overlay exists because `ViewApplicator` matches `getResourceKey()`
and `getReferenceKey()` against the _whole_ column path, so upstream's composite
expression `getResourceKey() = link.other.getReferenceKey()` cannot be
evaluated. It is split locally into two id-valued columns.

Splitting the _view_ is mechanical, but rewriting the _expectations_ is not:
upstream asserts `{"key_equal_ref": true}` while the split form asserts
`{"resourceKey": "p1", "referenceKey": "p1"}`. Deriving `"p1"` from `true` would
require re-implementing the key semantics that the conformance test exists to
check independently — the test would then assert that our implementation agrees
with a copy of our implementation. So the expectations stay a human claim about
the spec, recorded in a reviewed file.

### When the guard trips

1. Look at the upstream change to that file.
2. If our constraint still applies, update the overlay to carry the upstream
   change forward, then record the new upstream hash in the `.upstream.sha256`
   guard.
3. If the constraint is gone, delete **both** the overlay and its guard so the
   upstream file is vendored verbatim.

Removing the current overlay is tracked work: once `getResourceKey()` /
`getReferenceKey()` are evaluable as real sub-expressions, delete
`overlays/fn_reference_keys.json` and its guard.
