**NOTE**: This is a work in progress and the current version is only for
demonstration purposes. Once these tools reach Alpha status, this note should be
removed. This is a collaboration between Google and the OpenMRS community.

# What is this?

This repository includes pipelines to transform data from an
[OpenMRS](https://openmrs.org) instance using the
[FHIR format](https://www.hl7.org/fhir/overview.html) into a data warehouse
based on [Apache Parquet files](https://parquet.apache.org), or another FHIR
server (e.g., a HAPI FHIR server or Google Cloud FHIR store). There is also a
query library in Python to make working with FHIR based data warehouses simpler.

These tools are intended to be generic and eventually work with any FHIR-based
data source and data warehouse. Here is the list of main directories with a
brief description of their content:

- [pipelines/](pipelines/) Batch and streaming pipelines to transform data from 
  a FHIR based source to an analytics friendly data warehouse or another FHIR 
  store.

- [dwh/](dwh/) Query library for working with distributed FHIR-based data
  warehouses.

- [bunsen/](bunsen/) A fork of a subset of the
  [Bunsen](https://github.com/cerner/bunsen) project.

- [docker/](docker/) Docker configurations for various servers/pipelines.

- [doc/](doc/) Documentations

- [utils/](utils/) Various artifacts for setting up an initial database, running
  pipelines, etc.
  
- [e2e-tests/](e2e-tests/) Scripts for testing pipelines end-to-end.

- [synthea-hiv/](synthea-hiv/) Module to generate and upload synthetic patient data