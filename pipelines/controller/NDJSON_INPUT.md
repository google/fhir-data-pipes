# Using NDJSON Files as Input Source

The FHIR Pipelines Controller supports using NDJSON (Newline Delimited JSON) files as an input source for the data transformation pipeline. This provides flexibility when you need to process FHIR data that doesn't come directly from a FHIR server or database.

## Quick Start

**Controller Mode:**
```yaml
# application.yaml
fhirdata:
  fhirFetchMode: "NDJSON"
  sourceNdjsonFilePatternList: "/path/to/ndjson/*.ndjson"
  dwhRootPrefix: "dwh/ndjson_DWH"
  # List ALL resource types you want to process from your NDJSON files
  # Default is only "Patient,Encounter,Observation" if not specified!
  resourceList: "Patient,Encounter,Observation,Condition,Procedure,MedicationRequest"
  fhirVersion: "R4"
```

**Command Line Mode:**
```bash
java -jar batch-bundled.jar \
  --fhirFetchMode=NDJSON \
  --sourceNdjsonFilePatternList=/path/to/ndjson/*.ndjson \
  --outputParquetPath=/output/dwh \
  --resourceList=Patient,Encounter,Observation,Condition,Procedure,MedicationRequest \
  --fhirVersion=R4 \
  --runner=FlinkRunner
```

> **Important**: The `resourceList` parameter filters which resource types are extracted from your NDJSON files. Only listed resource types will be processed - all others will be ignored. If not specified, it defaults to only `"Patient,Encounter,Observation"`.

## Overview

NDJSON input mode allows you to:
- Process FHIR resources from files instead of live servers
- Import data from bulk exports, backups, or other static sources
- Work with data from systems that can generate NDJSON but don't have a full FHIR API
- Process data from cloud storage (GCS, S3) or local file systems

## Benefits

- **Flexibility**: No need for a running FHIR server or database
- **Simplicity**: Any system can generate NDJSON files
- **Portability**: Works with files from various sources and locations
- **Cloud Support**: Compatible with GCS and S3 storage

## NDJSON File Format

Each NDJSON file should contain FHIR resources in one of two formats:

### Format 1: Bundle Resource (Recommended)
```json
{
  "resourceType": "Bundle",
  "type": "collection",
  "entry": [
    {
      "resource": {
        "resourceType": "Patient",
        "id": "example-1",
        ...
      }
    },
    {
      "resource": {
        "resourceType": "Observation",
        "id": "example-2",
        ...
      }
    }
  ]
}
```

### Format 2: Line-Delimited Resources
Each line contains a single FHIR resource with no whitespace:
```
{"resourceType":"Patient","id":"example-1",...}
{"resourceType":"Observation","id":"example-2",...}
{"resourceType":"Condition","id":"example-3",...}
```

## Configuration

### 1. Set the Fetch Mode

In your `application.yaml` file, set `fhirFetchMode` to `NDJSON`:

```yaml
fhirdata:
  fhirFetchMode: "NDJSON"
```

### 2. Specify Input File Patterns

Set the `sourceNdjsonFilePatternList` to point to your NDJSON files. This accepts comma-separated file patterns:

```yaml
fhirdata:
  # Single directory
  sourceNdjsonFilePatternList: "/data/fhir/*.ndjson"

  # Or multiple directories
  # sourceNdjsonFilePatternList: "/data/patients/*.ndjson,/data/observations/*.ndjson"

  # Or cloud storage
  # sourceNdjsonFilePatternList: "gs://my-bucket/fhir-data/*.ndjson"
  # sourceNdjsonFilePatternList: "s3://my-bucket/fhir-data/*.ndjson"
```

### 3. Configure Output and Other Settings

Configure the remaining settings as needed:

```yaml
fhirdata:
  # Output directory for Parquet files
  dwhRootPrefix: "dwh/ndjson_DWH"

  # CRITICAL: Resource types to FILTER and process
  # ⚠️ IMPORTANT: Only resource types listed here will be extracted from your NDJSON files!
  # ⚠️ Default (if omitted) is ONLY "Patient,Encounter,Observation"
  # ⚠️ List ALL resource types present in your NDJSON files to avoid missing data
  resourceList: "Patient,Encounter,Observation,Condition,Procedure,MedicationRequest,DiagnosticReport,Practitioner,Organization,Location"

  # FHIR version
  fhirVersion: "R4"

  # Other settings...
  generateParquetFiles: true
  numThreads: 1
```

### 4. Validation Requirements

When using NDJSON mode:
- `sourceNdjsonFilePatternList` **must** be set and non-empty
- `fhirServerUrl` and `dbConfig` are **not required** (they will be ignored)
- All other standard configuration options remain available

## Complete Example Configuration

See [`config/application-ndjson-example.yaml`](config/application-ndjson-example.yaml) for a complete example configuration.

## File Pattern Syntax

The `sourceNdjsonFilePatternList` supports glob patterns:

| Pattern | Description | Example |
|---------|-------------|---------|
| `*.ndjson` | All .ndjson files in current directory | `/data/*.ndjson` |
| `**/*.ndjson` | All .ndjson files recursively | `/data/**/*.ndjson` |
| `file{1,2}.ndjson` | Multiple specific files | `/data/file{1,2}.ndjson` |
| `gs://bucket/path/*` | Cloud Storage (GCS) | `gs://my-bucket/fhir/*.ndjson` |
| `s3://bucket/path/*` | S3 Storage | `s3://my-bucket/fhir/*.ndjson` |

Multiple patterns can be combined using commas:
```yaml
sourceNdjsonFilePatternList: "/path/dir1/*.ndjson,/path/dir2/*.ndjson,gs://bucket/data/*.ndjson"
```

## Usage

### Option 1: Running via the Controller

1. Configure your `application.yaml` with NDJSON settings
2. Start the controller:
   ```bash
   mvn spring-boot:run
   ```
3. Open http://localhost:8080 in your browser
4. Click "Run Full" to process the NDJSON files

### Option 2: Running the Batch Pipeline Directly

You can also run the batch pipeline directly without the controller using the `java` command. This is useful for one-off processing or scripting.

#### Build the Batch Pipeline

First, build the batch pipeline JAR:

```bash
cd pipelines/batch
mvn clean install
```

#### Run with Java Command

Run the pipeline with command-line arguments:

```bash
java -jar target/batch-bundled.jar \
  --fhirFetchMode=NDJSON \
  --sourceNdjsonFilePatternList=/path/to/ndjson/*.ndjson \
  --outputParquetPath=/path/to/output/dwh \
  --resourceList=Patient,Encounter,Observation,Condition \
  --fhirVersion=R4 \
  --runner=FlinkRunner \
  --numThreads=4
```

#### Complete Example with Multiple Directories

```bash
java -jar target/batch-bundled.jar \
  --fhirFetchMode=NDJSON \
  --sourceNdjsonFilePatternList=/data/patients/*.ndjson,/data/observations/*.ndjson \
  --outputParquetPath=/output/fhir_dwh \
  --resourceList=Patient,Encounter,Observation,Condition,Practitioner,Location,Organization \
  --fhirVersion=R4 \
  --runner=FlinkRunner \
  --numThreads=4 \
  --structureDefinitionsPath=classpath:/r4-us-core-definitions \
  --recursiveDepth=1 \
  --rowGroupSizeForParquetFiles=33554432
```

#### Example with Cloud Storage

For Google Cloud Storage:

```bash
java -jar target/batch-bundled.jar \
  --fhirFetchMode=NDJSON \
  --sourceNdjsonFilePatternList=gs://my-bucket/fhir-data/*.ndjson \
  --outputParquetPath=gs://my-bucket/output/dwh \
  --resourceList=Patient,Encounter,Observation \
  --fhirVersion=R4 \
  --runner=FlinkRunner
```

For AWS S3:

```bash
java -jar target/batch-bundled.jar \
  --fhirFetchMode=NDJSON \
  --sourceNdjsonFilePatternList=s3://my-bucket/fhir-data/*.ndjson \
  --outputParquetPath=s3://my-bucket/output/dwh \
  --resourceList=Patient,Encounter,Observation \
  --fhirVersion=R4 \
  --runner=FlinkRunner
```

#### Key Command-Line Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--fhirFetchMode` | Set to NDJSON for NDJSON input | `NDJSON` |
| `--sourceNdjsonFilePatternList` | Comma-separated file patterns | `/data/*.ndjson` |
| `--outputParquetPath` | Output directory for Parquet files | `/output/dwh` |
| `--resourceList` | **FILTER** for resource types to process. ⚠️ Only listed types are extracted! Default: `Patient,Encounter,Observation` | `Patient,Observation,Condition` |
| `--fhirVersion` | FHIR version (R4 or DSTU3) | `R4` |
| `--runner` | Apache Beam runner (FlinkRunner or DirectRunner) | `FlinkRunner` |
| `--numThreads` | Number of parallel threads | `4` |
| `--structureDefinitionsPath` | Path to custom profiles | `classpath:/r4-us-core-definitions` |
| `--viewDefinitionsDir` | Directory with view definitions | `/config/views` |
| `--sinkDbConfigPath` | Database config for views | `/config/db-config.json` |

#### Using with ViewDefinitions

To generate views and write to a database:

```bash
java -jar target/batch-bundled.jar \
  --fhirFetchMode=NDJSON \
  --sourceNdjsonFilePatternList=/data/fhir/*.ndjson \
  --outputParquetPath=/output/dwh \
  --resourceList=Patient,Encounter,Observation \
  --fhirVersion=R4 \
  --runner=FlinkRunner \
  --viewDefinitionsDir=/path/to/views \
  --sinkDbConfigPath=/path/to/db-config.json \
  --recreateSinkTables=true
```

#### Memory Configuration

For large datasets, you may need to increase the JVM heap size:

```bash
java -Xmx8g -jar target/batch-bundled.jar \
  --fhirFetchMode=NDJSON \
  --sourceNdjsonFilePatternList=/data/large/*.ndjson \
  --outputParquetPath=/output/dwh \
  --resourceList=Patient,Encounter,Observation \
  --fhirVersion=R4
```

#### Script Example

Create a shell script for repeated use:

```bash
#!/bin/bash
# process-ndjson.sh

INPUT_DIR="/data/fhir/input"
OUTPUT_DIR="/data/fhir/output/dwh_$(date +%Y%m%d_%H%M%S)"
JAR_PATH="pipelines/batch/target/batch-bundled.jar"

java -Xmx4g -jar "$JAR_PATH" \
  --fhirFetchMode=NDJSON \
  --sourceNdjsonFilePatternList="$INPUT_DIR/*.ndjson" \
  --outputParquetPath="$OUTPUT_DIR" \
  --resourceList=Patient,Encounter,Observation,Condition,Practitioner \
  --fhirVersion=R4 \
  --runner=FlinkRunner \
  --numThreads=4

echo "Processing complete. Output written to: $OUTPUT_DIR"
```

Make it executable and run:

```bash
chmod +x process-ndjson.sh
./process-ndjson.sh
```

### Important Notes

- **Resource Filtering**: The `resourceList` parameter acts as a **filter** for which resource types to process from your NDJSON files.
  - **Default behavior**: If not specified, it defaults to `"Patient,Encounter,Observation"` - only these three resource types will be processed
  - **To process all resources**: You must explicitly list all resource types you want to extract from your NDJSON files
  - **Example**: If your NDJSON files contain Patient, Encounter, Observation, Condition, Medication, and Procedure resources, but you only set `resourceList: "Patient,Observation"`, only Patient and Observation resources will be extracted. The others will be ignored.
  - **Best practice**: List all resource types present in your NDJSON files, or you may miss data

  ```yaml
  # Process all common FHIR resource types
  resourceList: "Patient,Encounter,Observation,Condition,Procedure,MedicationRequest,DiagnosticReport,Practitioner,Organization,Location,AllergyIntolerance,Immunization,CarePlan,Goal"
  ```

- **Incremental Mode**: NDJSON mode reads static files, so incremental runs will re-process the same files unless you update the file patterns to point to new data. Consider triggering runs manually when new NDJSON files are available rather than relying on scheduled incremental runs.

- **File Validation**: Ensure your NDJSON files are valid JSON. Invalid files will cause errors during processing.

- **Performance**: Processing large numbers of files or very large files may require adjusting `numThreads` and memory settings.

## JSON Bundle Input Mode

In addition to NDJSON, you can also use JSON Bundle files as input. Each JSON file should contain a single FHIR Bundle resource.

### Configuration

```yaml
fhirdata:
  fhirFetchMode: "JSON"
  sourceJsonFilePatternList: "/path/to/bundles/*.json"
```

See [`config/application-json-example.yaml`](config/application-json-example.yaml) for a complete example.

### Running JSON Mode via Command Line

```bash
java -jar target/batch-bundled.jar \
  --fhirFetchMode=JSON \
  --sourceJsonFilePatternList=/path/to/bundles/*.json \
  --outputParquetPath=/output/dwh \
  --resourceList=Patient,Encounter,Observation \
  --fhirVersion=R4 \
  --runner=FlinkRunner
```

## Differences from Other Input Modes

| Feature | NDJSON/JSON Mode | FHIR Search | Bulk Export | JDBC |
|---------|------------------|-------------|-------------|------|
| Requires FHIR Server | No | Yes | Yes | No |
| Requires Database Access | No | No | No | Yes |
| Supports Incremental Sync | Limited* | Yes | Yes | Yes |
| Works with Static Files | Yes | No | Partially** | No |
| Cloud Storage Support | Yes | No | No | No |

\* Incremental sync requires managing file patterns to point to new data
\*\* Bulk Export generates NDJSON files but triggers the export from the server

## Troubleshooting

### Error: "sourceNdjsonFilePatternList cannot be empty for NDJSON fetch mode"
**Solution**: Set `sourceNdjsonFilePatternList` in your `application.yaml` file.

### Missing Resources / Fewer Resources Than Expected
This is the most common issue with NDJSON input!

**Problem**: You have resources in your NDJSON files, but they're not appearing in the output.

**Solution**: Check your `resourceList` configuration:
```bash
# First, inspect what resource types are in your NDJSON files
cat /path/to/file.ndjson | jq -r '.resourceType' | sort -u

# Or if using Bundle format:
cat /path/to/file.ndjson | jq -r '.entry[].resource.resourceType' | sort -u
```

Then update your `resourceList` to include ALL resource types you want to process:
```yaml
# Before (only processes 3 types):
resourceList: "Patient,Encounter,Observation"

# After (processes all types in your files):
resourceList: "Patient,Encounter,Observation,Condition,Procedure,MedicationRequest,AllergyIntolerance,DiagnosticReport,Practitioner,Organization,Location"
```

**Remember**: `resourceList` is a FILTER, not a discovery mechanism. Only explicitly listed resource types will be processed.

### No Resources Processed At All
**Solution**:
- Check that your file patterns match actual files
- Verify resource types in files match those in `resourceList`
- Ensure NDJSON files are properly formatted
- Check that files are readable (permissions)

### Out of Memory Errors
**Solution**:
- Increase JVM heap size: `java -Xmx4g -jar controller-bundled.jar`
- Reduce `numThreads` setting
- Process files in smaller batches

### Invalid JSON Errors
**Solution**:
- Validate your NDJSON files using a JSON validator
- Ensure there are no extra whitespace or newlines within resource objects
- Check that each line is a complete, valid JSON object

## Related Documentation

- [FHIR Batch Pipeline README](../batch/README.md) - Details on the underlying batch pipeline
- [Main Pipelines README](../README.md) - Overview of all pipeline modes
- [Example Configurations](config/) - Sample configuration files

## Support

For issues or questions:
- Open an issue on [GitHub](https://github.com/google/fhir-data-pipes/issues)
- Reference issue [#1197](https://github.com/google/fhir-data-pipes/issues/1197) for NDJSON input support
