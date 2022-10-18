<!-- This is based on openmrs-cord PR template. -->

## Description of what I changed

<!--- Describe your changes in detail -->
<!--- It can simply be your commit message, which you must have -->
<!--- If your PR is related to an issue , please mention it here. -->
<!--- It is generally a good practice to first file an issue with enough
  context and reference it in the PR, but if you don't have that, please remove
  this section. -->

## E2E test

<!-- There are different scenarios for using the tools in this repo; please
  help your reviewers by describing how you have e2e tested your change. -->

TESTED:

Please replace this with a description of how you tested your PR beyond the
automated e2e/unit tests.

## Checklist: I completed these to help reviewers :)

<!--- Put an `x` in the box if you did the task -->
<!--- If you forgot a task please follow the instructions below -->

- [ ] I have read and will follow the
      [review process](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/blob/master/doc/review_process.md).
- [ ] I am familiar with Google Style Guides for the language I have coded in.

  No? Please take some time and review
  [Java](https://google.github.io/styleguide/javaguide.html) and
  [Python](https://google.github.io/styleguide/pyguide.html) style guides. Note,
  when in conflict, OpenMRS style guide overrules.

- [ ] My IDE is configured to follow the Google
      [**code styles**](https://google.github.io/styleguide/).

  No? Unsure? ->
  [configure your IDE](https://github.com/google/google-java-format).

- [ ] I have **added tests** to cover my changes. (If you refactored existing
      code that was well tested you do not have to add tests)
- [ ] I ran `mvn clean package` right before creating this pull request and
      added all formatting changes to my commit.
- [ ] All new and existing **tests passed**.
- [ ] My pull request is **based on the latest changes** of the master branch.

  No? Unsure? -> execute command `git pull --rebase upstream master`
