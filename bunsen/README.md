This is a fork of a subset of the
[Bunsen project](https://github.com/cerner/bunsen). This is created to unblock
us for pushing changes that are needed in the Bunsen codebase. For example:

- Make exported resource IDs consistent (Issue
  [#55](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/55)).
  This is fixed by
  [PR #111 in Bunsen](https://github.com/cerner/bunsen/pull/111) but not merged.
- AvroTypeException for some edge cases (Issue
  [#156](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/156))
  which needs a fix in Bunsen.

When development resumes on Bunsen or at least our PRs are reviewed/merged, we
can drop this fork and go back to directly use Bunsen artifacts. For that reason
we try to keep this copy as close as possible to the up-stream repo such that
commits here can easily be pushed to Bunsen in the future.

**Note**: Both projects use the same Apache v2.0 license.
