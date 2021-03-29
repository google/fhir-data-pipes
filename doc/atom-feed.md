# Atom Feed Mode

**NOTE:** The Atom Feed mode is deprecated and may be removed in a future
version. We strongly recommend using one of the other modes instead.

**NOTE:** The Atom Feed mode does not support Parquet files.

The Atom Feed mode listens to the
[Atom Feed module of OpenMRS](https://wiki.openmrs.org/display/docs/Atom+Feed+Module)
for new or modified data. The feed is modified to also include URLs to the
corresponding FHIR resources.

## Add the Atom Feed module to OpenMRS

Install the
[Atom Feed Module](https://addons.openmrs.org/show/org.openmrs.module.atomfeed)
if necessary.

## Update the Atom Feed configuration

Next, update the Atom Feed module to include FHIR resource URLs in its output.
From the OpenMRS home page click **Atomfeed > Load Configuration > Choose
File**. Select
[`utils/fhir2_atom_feed_config.json`](utils/fhir2_atom_feed_config.json)
provided in this repository then click **Import**.

To test, visit the Atom Feed URL for a resource, e.g.
[http://localhost:8099/openmrs/ws/atomfeed/patient/1](). Find an `entry` and
it's `content` tag. Find the `fhir` path, append it to your OpenMRS URL, and
open it. It should download the associated FHIR resource.

For example, if OpenMRS is running at [http://localhost:8099/openmrs]() and this
is your content tag:

```xml
<content type="application/vnd.atomfeed+xml"><![CDATA[{"rest":"/ws/rest/v1/patient/ab6cba2e-8e03-41ec-a551-aab6e04b27f6?v=full","fhir":"/ws/fhir2/R4/Patient/ab6cba2e-8e03-41ec-a551-aab6e04b27f6"}]]></content>
```

Then `/ws/fhir2/R4/Patient/ab6cba2e-8e03-41ec-a551-aab6e04b27f6` is your `fhir`
path and
[http://localhost:8099/openmrs/ws/fhir2/R4/Patient/ab6cba2e-8e03-41ec-a551-aab6e04b27f6]()
is the URL for the FHIR resource.

## Set up the Atom Feed client side database

The Atom Feed-based streaming client requires a MySQL database to store failed
events and progress markers.

If you don't have an available MySQL service, you can start one up using Docker:

```shell
$ docker run --name=atomfeed-db -d mysql/mysql-server:latest \
  -p 127.0.0.1:3306:3306 -e "MYSQL_ROOT_PASSWORD=root" \
  -e "MYSQL_USER=fhir" -e "MYSQL_PASSWORD=fhiranalytics"
```

This will create the MySQL user `fhir` with password `fhiranalytics` which is
used in the following code and examples. Adjust these values as necessary.

Next, create the necessary database and tables. From your MySQL server run:

```shell
mysql --user=fhir --password=fhiranalytics < utils/dbdump/atomfeed_db_sql
```

The default database name is `atomfeed_client`. You can change this by editing
[`utils/dbdump/atomfeed_db_sql`](utils/dbdump/atomfeed_db_sql).

Finally, update
[`src/main/resources/hibernate.default.properties`](src/main/resources/hibernate.default.properties)
with any customizations you've made. Pay extra attention to
`hibernate.connection.url`, `hibernate.connection.username`, and
`hibernate.connection.password`.

## Compile and run

From the root of your git repo, compile the client:

```
mvn clean install -pl streaming-atomfeed -am
```

Based on the flags and their values, the client will infer what the target is.

**Using a FHIR server**

```
mvn exec:java -pl streaming-atomfeed \
    -Dexec.args=" --openmrsUserName=admin --openmrsPassword=Admin123 \
    --openmrsServerUrl=http://localhost:8099/openmrs \
    --fhirSinkPath=http://localhost:8098/fhir \
    --sinkUserName=hapi --sinkPassword=hapi"`
```

**Using GCP FHIR store**

```
mvn exec:java -pl streaming-atomfeed \
    -Dexec.args="--openmrsUserName=admin --openmrsPassword=Admin123 \
    --openmrsServerUrl=http://localhost:8099/openmrs \
    --fhirSinkPath=projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME"`
```

See [Common Parameters](#common-parameters) for information about the
parameters.

To test your changes, create a new patient (or observation) in OpenMRS and check
that a corresponding Patient (or Observation) FHIR resource is created in the
GCP FHIR store and corresponding rows added to the BigQuery tables.
