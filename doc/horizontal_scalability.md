# Horizontal Scalability in FHIR Data Pipes

Horizontal scalability allows FHIR Data Pipes to process large volumes of FHIR resources by distributing workload across multiple machines or workers. This enables faster data ingestion and transformation, particularly when working with large clinical datasets.

## Why Horizontal Scalability Matters
- FHIR servers can contain millions of resources.
- Single-machine processing becomes slow for analytics workloads.
- Distributed pipelines help parallelize:
  - Resource extraction
  - Transformation
  - Data writing to Parquet or another FHIR store

## How FHIR Data Pipes Scales Horizontally
FHIR Data Pipes supports horizontal scalability through:
1. **Apache Beam Runner Scaling**
   - Beam supports distributed runners like:
     - Google Dataflow
     - Flink
     - Spark
   - These runners automatically scale workers based on job load.

2. **Sharding FHIR Resources**
   - Resources can be split by:
     - Resource type
     - Logical ID ranges
     - Page tokens
   - Each shard is processed independently.

3. **Parallel I/O**
   - Multiple workers fetch resources from FHIR store in parallel.
   - Writes to Parquet or downstream systems are also parallelized.

4. **Stateless Transformations**
   - Most transformations in pipelines are stateless.
   - This allows Beam to distribute them across workers without coordination issues.

## Recommended Deployment Pattern
To scale horizontally:
1. Choose a distributed runner (Dataflow recommended for GCP users).
2. Enable autoscaling.
3. Increase worker limits depending on dataset size.
4. Use sharded execution for large Patient-centric datasets.
5. Monitor worker performance using runner-specific dashboards.

## Example High-Level Architecture
