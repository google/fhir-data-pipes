# Automatic `VIEW` registration

This directory contains sample view definition files for automatic registration.
For each resource type (e.g., `Patient`) all files starting with that resource
type name and ending in `.sql` (e.g., `Patient_flat.sql`) are expected to
contain a `CREATE VIEW` query.

To enable this feature, the configuration parameter `createHiveResourceTables`
should be set and `hiveResourceViewsDir` should point to this directory. In this
case, after each pipeline run, the controller reads the content of this
directory and for enabled resources, create their corresponding views. The
sample files here are named after the `VIEW` that they define but this is not
required.
