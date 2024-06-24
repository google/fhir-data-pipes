# SQL-on-FHIR-v2 repo tests

Test files in this directory are copied from test `/tests` directory of the
[SQL-on-FHIR v2 repository](https://github.com/FHIR/sql-on-fhir-v2). They are
used to make sure that our implementation complies with the spec. The reason
that we manually copy these files (instead of an automated way or a submodule)
is that we don't want our continuous-build suddenly fail because of SoFv2 repo
changes. Also, there are certain tests that we need to exclude for various
reasons (see
[SKIPPED_TESTS here](https://github.com/google/fhir-data-pipes/blob/master/pipelines/common/src/test/java/com/google/fhir/analytics/view/SQLonFHIRv2Test.java#L64));
and we make some changes to `fn_reference_keys.json` because we have special
constraints on `getResourceKey()` and `getReferenceKey()` in FHIRPaths.

Whenever we re-import test files for the SoFv2 repo, we should regenerate the
`sql-on-fhir-v2-test-result.json` file as well (which is placed in the parent
directory not to be confused with a test case). This file is referenced in the
sql-on-fhir-v2 repo's
[implementations.json](https://github.com/FHIR/sql-on-fhir-v2/blob/master/implementations.json).
It is auto-generated in the Java temp directory (e.g., `/tmp`), after each
[`SQLonFHIRv2Test`](https://github.com/google/fhir-data-pipes/blob/master/pipelines/common/src/test/java/com/google/fhir/analytics/view/SQLonFHIRv2Test.java)
run.
