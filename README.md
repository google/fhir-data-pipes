[![Build
Status](https://badger-zpct3epzcq-ue.a.run.app/build/status?project=cloud-build-fhir&id=4b13d289-3b1e-4a45-aa86-8166d5a5f481)](https://storage.googleapis.com/cloud-build-gh-logs/README.html)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/openmrs-fhir-analytics/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/openmrs-fhir-analytics)

**NOTE**: This is a work in progress and the current version is not ready for
production use. Once these tools reach Alpha status, this note should be
removed. This was originally started as a collaboration between Google and
the OpenMRS community.

# What is this?

This repository includes pipelines to transform data from a [FHIR](https://hl7.org/fhir/)
server (like [HAPI](https://hapifhir.io/),
[GCP FHIR store](https://cloud.google.com/healthcare-api/docs/concepts/fhir#fhir_stores),
or even [OpenMRS](https://openmrs.org)) using the
[FHIR format](https://www.hl7.org/fhir/overview.html) into a data warehouse
based on [Apache Parquet files](https://parquet.apache.org), or another FHIR
server. There is also a query library in Python to make working with FHIR-based
data warehouses simpler.

These tools are intended to be generic and eventually work with any FHIR-based
data source and data warehouse. Here is the list of main directories with a
brief description of their content:

- [pipelines/](pipelines/) Batch and streaming pipelines to transform data from a FHIR-based source to an analytics-friendly data warehouse or another FHIR store.

- [dwh/](dwh/) Query library for working with distributed FHIR-based data
  warehouses.

- [bunsen/](bunsen/) A fork of a subset of the
  [Bunsen](https://github.com/cerner/bunsen) project.

- [docker/](docker/) Docker configurations for various servers/pipelines.

- [doc/](doc/) Documentations

- [utils/](utils/) Various artifacts for setting up an initial database, running
  pipelines, etc.
  
- [e2e-tests/](e2e-tests/) Scripts for testing pipelines end-to-end.
