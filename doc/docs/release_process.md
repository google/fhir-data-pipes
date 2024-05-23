# Semantic versioning

Versioning across all Open Health Stack components is based on the major.minor.patch scheme and respects Semantic Versioning.

Respecting Semantic Versioning is important for multiple reasons:

*   It guarantees simple minor version upgrades, as long as you only use the public APIs
*   A new major version is an opportunity to thoroughly document breaking changes
*   A new major/minor version is an opportunity to communicate new features through a blog post

## Major versions
The major version number is incremented on every breaking change.

Whenever a new major version is released, we publish:

*   a blog post with feature highlights, major bug fixes, breaking changes, and upgrade instructions.
*   an exhaustive changelog entry via the release notes

## Minor versions
The minor version number is incremented on every significant retro-compatible change.

Whenever a new minor version is released, we publish:

*   an exhaustive changelog entry via the release notes

## Patch versions
The patch version number is incremented on bugfixes releases.

Whenever a new patch version is released, we publish:

*   an exhaustive changelog entry