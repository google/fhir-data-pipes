[![Build Status](https://travis-ci.org/GoogleCloudPlatform/openmrs-fhir-analytics.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/openmrs-fhir-analytics)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/openmrs-fhir-analytics/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/openmrs-fhir-analytics)

**NOTE**: This is a work in progress and the current version is only for
demonstration purposes. Once these tools reach Alpha status, this note should be
removed. This is a collaboration between Google and OpenMRS community and this
repository might be moved to [OpenMRS org](https://github.com/openmrs) in the
future.

# What is this?

This repository includes tools that manage and export data from an
[OpenMRS](https://openmrs.org) instance using the
[FHIR format](https://www.hl7.org/fhir/overview.html) and facilitate the
transfer to a data warehouse using
[Apache Parquet files](https://parquet.apache.org), or another FHIR server
(e.g., a HAPI FHIR server or Google Cloud FHIR store). The tools use the
[OpenMRS FHIR2 Module](https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module)
to transform the data for transfer.

There are four modes of transfer:

-   [**Batch mode (FHIR)**](#batch-mode-using-fhir-search): This mode uses FHIR
    Search APIs to select resources to copy, retrieves them as FHIR resources,
    and transfers the data via FHIR APIs or Parquet files.
-   [**Batch mode (JDBC)**](#batch-mode-using-jdbc): This mode uses JDBC to read
    the IDs of entities in an OpenMRS MySQL database, retrieves those entities
    as FHIR resources, and transfers the data via FHIR APIs or Parquet files.
-   **[Streaming mode (Debezium)](#streaming-mode-debezium)**: This mode
    continuously listens for changes to the underlying OpenMRS MySQL database
    using
    [Debezium](https://debezium.io/documentation/reference/1.2/connectors/mysql.html).
-   **[Streaming mode (Atom Feed)](#streaming-mode-atom-feed)**: This mode
    continuously listens for changes reported by the OpenMRS API using the
    [Atom Feed module of OpenMRS](https://wiki.openmrs.org/display/docs/Atom+Feed+Module).

There is also a query module built on top of the generated data warehouse.

## Prerequisites

-   An [OpenMRS](https://openmrs.org) instance with the latest version of the
    [FHIR2 Module](https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module)
    installed.
    -   There is an [OpenMRS Reference Application](#run-openmrs-using-docker)
        image with these prerequisites and demo data you can use to try things
        out.
-   A target output for the data. Supported options are Apache Parquet files
    (not supported by Atom Feed mode) or a FHIR server such as HAPI FHIR or
    [Google Cloud Platform FHIR stores](https://cloud.google.com/healthcare/docs/how-tos/fhir).
    -   You can use our [HAPI FHIR server](#run-hapi-fhir-server-using-docker)
        image for testing FHIR API targets.
    -   [Learn how to create a compatible GCP FHIR store](#create-a-google-cloud-fhir-store-and-bigquery-dataset),
        if you want to use this option.

### Installing the FHIR2 Module

**To install via web**: Log in to OpenMRS as an administrator, go to **System
Administration > Manage Modules > Search from Addons**, search for the `fhir2`
module, and install. Make sure to install the `fhir2` module, not `fhir`.

**To install via .omod file**: Go to
[FHIR2 Module](https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module)
and download the .omod file. Go to **Advanced Administration > Module
Properties** and find the modules folder. Move the .omod file to the modules
folder then restart OpenMRS.

[Learn more about OpenMRS modules](https://guide.openmrs.org/en/Configuration/customizing-openmrs-with-plug-in-modules.html).

## Setup

1.  [Install the FHIR2 Module](#installing-the-fhir2-module) if necessary.
2.  [Clone](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository)
    the
    [OpenMRS FHIR Analytics](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics)
    project to your machine.
3.  Set the `utils` directory to world-readable: `chmod -r 755 ./utils`.
4.  Build binaries with `mvn clean install`.

## Common Parameters

Although each mode of transfer uses a different binary, they use some common
parameters which are documented here.

-   `openmrsServerUrl` - The base URL of the source OpenMRS instance. Default:
    `http://localhost:8099/openmrs`
-   `openmrsUserName` - The HTTP Basic Auth username to access the OpenMRS APIs.
    Default: `admin`
-   `openmrsPassword` - The HTTP Basic Auth password to access the OpenMRS APIs.
    Default: `Admin123`
-   `fhirSinkPath` - A base URL to a target FHIR server, or the relative path of
    a GCP FHIR store, e.g. `http://localhost:8098/fhir` for a FHIR server or
    `projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIR-STORE-NAME`
    for a GCP FHIR store.
-   `sinkUserName` - The HTTP Basic Auth username to access the FHIR sink. Not
    used for GCP FHIR stores.
-   `sinkPassword` - The HTTP Basic Auth password to access the FHIR sink. Not
    used for GCP FHIR stores.
-   `outputParquetPath` - The file path to write Parquet files to, e.g.,
    `./tmp/parquet/`.

## Batch mode

Batch mode can use two different methods to query the source database. By
default it uses [FHIR Search](https://www.hl7.org/fhir/search.html) API calls to
find the resources to copy. Alternatively, it can use
[JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) to query
the OpenMRS MySQL database directly.

### Using Batch mode

First, complete the [Setup](#setup) instructions.

The next sections describe parameters specific to Batch mode. See
[Common Parameters](#common-parameters) for information about the other
parameters.

### Batch mode using FHIR Search

To start Batch Mode using FHIR Search, run:

```shell
$ java -cp batch/target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --openmrsServerUrl=http://localhost:8099/openmrs \
    --openmrsUserName=admin --openmrsPassword=Admin123 \
    --fhirSinkPath=http://localhost:8098/fhir \
    --sinkUserName=hapi --sinkPassword=hapi \
    --outputParquetPath=/tmp/TEST/ \
    --searchList=Patient,Encounter,Observation --batchSize=20
```

Parameters:

-   `searchList` - A comma-separated list of
    [FHIR Search](https://www.hl7.org/fhir/search.html) URLs. For example,
    `Patient?given=Susan` will extract only Patient resources that meet the
    `given=Susan` criteria. Default: `Patient,Encounter,Observation`
-   `batchSize` - The number of resources to fetch in each API call.
    Default: `100`
-   `serverFhirEndpoint` - The OpenMRS server base path for its FHIR API
    endpoints. Using all default values, you would find Patient resources at
    `http://localhost:8099/openmrs/ws/fhir2/R4/Patient`. This generally should
    not be changed. Default: `/ws/fhir2/R4`

### Batch mode using JDBC

To start Batch Mode using JDBC, run:

```shell
$ java -cp batch/target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --openmrsServerUrl=http://localhost:8099/openmrs \
    --openmrsUserName=admin --openmrsPassword=Admin123 \
    --fhirSinkPath=http://localhost:8098/fhir \
    --sinkUserName=hapi --sinkPassword=hapi \
    --outputParquetPath=/tmp/TEST/ \
    --searchList=Patient,Encounter,Observation --batchSize=20 \
    --jdbcModeEnabled=true --jdbcUrl=jdbc:mysql://localhost:3306/openmrs \
    --dbUser=root --dbPassword=debezium --jdbcMaxPoolSize=50 \
    --jdbcDriverClass=com.mysql.cj.jdbc.Driver

```

Parameters:

-   `searchList` - A comma-separated list of FHIR Resources to be fetched from
    OpenMRS. Default: `Patient,Encounter,Observation`
-   `batchSize` - The number of resources to fetch in each API call. Default:
    `100`
-   `serverFhirEndpoint` - The OpenMRS server base path for its FHIR API
    endpoints. Using all default values, you would find Patient resources at
    `http://localhost:8099/openmrs/ws/fhir2/R4/Patient`. This generally should
    not be changed. Default: `/ws/fhir2/R4`
-   `jdbcModeEnabled` - If true, uses JDBC mode. Default: `false`
-   `jdbcUrl` - The
    [JDBC database connection URL](https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html#db_connection_url)
    for the OpenMRS MySQL database. Default:
    `jdbc:mysql://localhost:3306/openmrs`
-   `dbUser` - The name of a user that has access to the database. Default:
    `root`
-   `dbPassword` - The password of `dbUser`. Default: `debezium`
-   `jdbcMaxPoolSize` - The maximum number of database connections. Default:
    `50`
-   `jdbcDriverClass` - The fully qualified class name of the JDBC driver. This
    generally should not be changed. Default: `com.mysql.cj.jdbc.Driver`

## Streaming mode (Debezium)

The Debezium-based streaming mode provides real-time downstream consumption of
incremental updates, even for operations that were performed outside the OpenMRS
API, e.g., data cleaning, module operations, and data syncing/migration. It
captures incremental updates from the MySQL database binlog then streams both
FHIR and non-FHIR data for downstream consumption. It is not based on Hibernate
Interceptor or Event Module; therefore, all events are captured from day 0 and
can be used independently without the need for a batch pipeline. It also
tolerates failures like application restarts or crashes, as the pipeline will
resume from the last processed offset.

### Getting Started

1.  Complete the Debezium
    [MySQL Setup](https://debezium.io/documentation/reference/1.4/connectors/mysql.html#setting-up-mysql).
    At a minimum,
    [create a user](https://debezium.io/documentation/reference/1.4/connectors/mysql.html#mysql-creating-user)
    and
    [enable the binlog](https://debezium.io/documentation/reference/1.4/connectors/mysql.html#enable-mysql-binlog).
    (The test [Docker image](#run-openmrs-and-mysql-using-docker) has already
    done these steps.)
2.  Edit `./utils/dbz_event_to_fhir_config.json`. Find the
    `debeziumConfigurations` section at the top of the file and edit the values
    to match your environment. See the documentation on
    [Debezium MySQL Connector properties](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-property-name)
    for more information.
3.  Build binaries with `mvn clean install`.
4.  Run the pipeline to a FHIR server and Parquet files:

    ```shell
    $ mvn compile exec:java -pl streaming-binlog \
        -Dexec.args="--openmrsServerUrl=http://localhost:8099/openmrs \
        --openmrsUserName=admin --openmrsPassword=Admin123 \
        --fhirSinkPath=http://localhost:8098/fhir \
        --sinkUserName=hapi --sinkPassword=hapi \
        --outputParquetPath=/tmp/TEST/ \
        --fhirDebeziumConfigPath=./utils/dbz_event_to_fhir_config.json \
        --openmrsfhirBaseEndpoint=/ws/fhir2/R4"
    ```

    Or to a GCP FHIR store:

    ```shell
    $ mvn compile exec:java -pl streaming-binlog \
        -Dexec.args="--openmrsServerUrl=http://localhost:8099/openmrs \
        --openmrsUserName=admin --openmrsPassword=Admin123 \
        --fhirSinkPath=projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME \
        --fhirDebeziumConfigPath=./utils/dbz_event_to_fhir_config.json \
        --openmrsfhirBaseEndpoint=/ws/fhir2/R4"
    ```

The next sections describe parameters specific to Debezium Streaming mode. See
[Common Parameters](#common-parameters) for information about the other
parameters.

Parameters:

-   `fhirDebeziumConfigPath` - The path to the configuration file containing
    MySQL parameters and FHIR mappings. This generally should not be changed.
    Default: `utils/dbz_event_to_fhir_config.json`
-   `openmrsfhirBaseEndpoint` - The OpenMRS server base path for its FHIR API
    endpoints. Using all default values, you would find Patient resources at
    `http://localhost:8099/openmrs/ws/fhir2/R4/Patient`. This generally should
    not be changed. Default: `/ws/fhir2/R4`

If `outputParquetPath` is set, there are additional parameters:

-   `secondsToFlushParquetFiles` - The number of seconds to wait before flushing
    all Parquet writers with non-empty content to files. Use `0` to disable.
    Default: `3600`.
-   `rowGroupSizeForParquetFiles` - The approximate size in bytes of the
    row-groups in Parquet files. When this size is reached, the content is
    flushed to disk. This is not used if there are less than 100 records. Use
    `0` to use the default Parquet row-group size. Default: `0`.

### Common questions

*   **Will this pipeline include historical data recorded before the `mysql
    binlog` was enabled?** Yes. By default, the pipeline takes a snapshot of the
    entire database and will include all historical data.

*   **How do I stop Debezium from taking a snapshot of the entire database?**
    Set `snapshotMode` in the config file to `schema_only` i.e,
    `"snapshotMode" : "initial"`. Other options include: `when_needed`,
    `schema_only`, `initial` (default), `never`, e.t.c. See the
    [`debezium documentation`](https://camel.apache.org/components/latest/debezium-mysql-component.html)
    for more details.

## Streaming mode (Atom Feed)

**NOTE:** The Atom Feed mode is deprecated and may be removed in a future
version. We strongly recommend using one of the other modes instead.

**NOTE:** The Atom Feed mode does not support Parquet files.

[See here](doc/atom-feed.md) for more information.

## Using Docker compose

You can run the entire pipeline using Docker containers by following these
instructions.

### Run OpenMRS and MySQL using Docker

```
$ docker-compose -f docker/openmrs-compose.yaml up
```

Once running you can access OpenMRS at <http://localhost:8099/openmrs/> using
username "admin" and password "Admin123". The Docker image includes the required
FHIR2 module and demo data. Edit `docker/openmrs-compose.yaml` to change the default
port.

If `docker-compose` fails, you may need to adjust file permissions.

```
$ docker-compose -f docker/openmrs-compose.yaml down -v
$ chmod a+rx ./utils ./utils/dbdump
$ chmod -R a+r ./utils
$ docker-compose -f docker/openmrs-compose.yaml up
```

In order to see the demo data in OpenMRS you must rebuild the search index. In
OpenMRS go to **Home > System Administration > Advanced Administration**. Under
**Maintenance** go to **Search Index** then **Rebuild Search Index**.

### (Optional) Run HAPI FHIR server using Docker

This is only needed if you want to test sending data to a FHIR server and is not
needed if you are only generating Parquet files.

```
$ docker-compose -f docker/sink-compose.yml up
```

You can access the FHIR server at <http://localhost:8098/fhir/>. Edit
`sink-compose.yml` to change the default port.

### Configure docker-compose.yaml

Parameters (e.g port/url) have been configured to point to
http://localhost:8099/openmrs/

Remember to appropriately change other parameters such as source url, sink path,
and any required authentication information.

### Run a Transfer Pipeline

#### Batch Pipeline

```
$ mvn clean install
$ docker-compose -f docker/docker-compose.yaml up --build batch
```

##### Streaming Pipeline (Debezium)

```
$ mvn clean install
$ docker-compose -f docker/docker-compose.yaml up --build streaming-binlog
```

##### Streaming Pipeline (Atomfeed)

```
 $ mvn clean install -pl streaming-atomfeed -am
 $ docker-compose -f docker/docker-compose.yaml up -d --build streaming-atomfeed-db streaming-atomfeed
```

## How to query the data warehouse

Here we only focus on the case where the FHIR resources are transformed into
Parquet files in previous steps and we want to query them through Spark.

Some sample queries using `pyspark` are available in the [`dwh/`](dwh) folder.

### Setting up Apache Spark

Details on how to set up a Spark cluster with all different cluster management
options is beyond this doc. Here we only show how to run the sample queries in
the local mode.

The sample queries are written using the Python API of Spark (a.k.a. `pyspark`)
so it is a good idea to first create a Python `virtualenv`:

```
$ virtualenv -p python3.8 venv
$ . ./venv/bin/activate
(venv) $ pip3 install pyspark
(venv) $ pip3 install pandas
```

Or you can just do `pip3 install -r requirements.txt` in the `venv`.

*Tip*: The initial step of the `validate_indicators.sh` script creates a
`venv_test` and sets up all the dependencies.

Once this is done, you can run `pyspark` to get into an interactive console.
This is useful for developing queries step by step, but we won't use it once
we have developed the full query plan.

Some test Parquet files are stored in the `dwh/test_files` directory. These are
generated by the batch pipeline from the demo data in the test docker image.
In particular, there are `Patient` and `Observation` sub-folders with some
Parquet files in them. Here is a sample command for the indicator calculation
script:

```
spark-submit indicators.py --src_dir=./test_files --last_date=2020-12-30 \
  --num_days=28 --output_csv=TX_PVLS.csv
```

To see a list of options with their descriptions, try:

```
$ python3 indicators.py --help
```

There is also a [`sample_indicator.py`](dwh/sample_indicator.py) script to
show how the same query with PySpark API can be done with Spark SQL:

```
spark-submit sample_indicator.py --src_dir=test_files/ \
  --base_patient_url=http://localhost:8099/openmrs/ws/fhir2/R4/Patient/ \
  --num_days=60 --last_date=2020-04-30
  --code_list {SPACE SEPARATED LIST OF CODES}
```

### Using Jupyter Notebooks

While in your virtualenv, run:

```bash
(venv) $ pip3 install jupyter
```

Then from a terminal that can run GUI applications, run:

```bash
(venv) $ jupyter notebook
```

This should start a Jupyter server and bring up its UI in a browser tab.

In case that auto-completion functionality is not working, this might be due
to a recent regression; check the solution
[here](https://github.com/ipython/ipython/issues/12740#issuecomment-751273584)
to downgrade your `jedi`.

Also if you like to automatically record the execution time of different cells
(and other nice features/extensions) install
[nbextensions](https://jupyter-contrib-nbextensions.readthedocs.io/en/latest/install.html).

A sample notebook is provided at [`dwh/test_spark.ipynb`](dwh/test_spark.ipynb)
which uses `matplotlib` too, so to run this you need to install it too:

```bash
(venv) $ pip3 install matplotlib
```

At the end of this notebook, we wrap up the tests
into reusable functions that we can use outside the Jupyter environment e.g.,
for automated indicator computation.
