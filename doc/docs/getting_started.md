# Getting Started

In this section we will run through concepts related to FHIR Data Pipes and instructions for getting started quickly.

## FHIR Data Pipes concepts
FHIR Data Pipes is built on technology **designed for horizontal scalability** and has multiple deployment options depending on your use case and requirements.

FHIR Data Pipes is made up of:

*   **FHIR Data Pipes Pipelines**: Java JAR which moves and transforms data from a FHIR server to Apache Parquet files for data analysis or another FHIR server for data integration. Pipelines is built using Apache Beam and can run on a single machine or cluster.

*   **The Pipelines Controller module**: A user-interface wrapper for the FHIR Data Pipes Pipelines, integrating "full", "incremental", and "merger" pipelines together. Using the controller module you can schedule periodic incremental updates or use a web control panel to start the pipeline manually. The Pipelines Controller is built on top of pipelines and shares many of the same settings

*   **Single machine Docker Compose configuration**: Runs the Pipelines Controller plus a Spark Thrift server so you can more easily run SparkSQL queries on the Parquet files output by the Pipelines Controller. It can automatically create and update tables on the Spark Thrift server each time the Pipelines Controller runs the data pipeline, simplifying data analysis or dashboard creation. This configuration also serves as a very simple example of how to integrate the Parquet output of Pipelines in a Spark cluster environment.

## Install FHIR Data Pipes
There are a number of ways to FHIR Data Pipes can be installed and deployed on a number of different platforms and using different architectural patterns depending on the environment, amount of data and requirements for horizontal scalability.

You can find additional information and instructions in the following sections:

*   Configuring the FHIR Data Pipes Pipeline and Controller Module
*   FHIR Data Pipes docker containers
*   Single machine deployment (for quick set-up)

Once you have set-up and deployed the FHIR Data Pipes Pipelines, you can explore different [tutorials](../tutorials.md) for working with FHIR Data Pipes.