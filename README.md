**NOTE**: This is a work in progress and the current version is only for
demonstration purposes. Once these tools reach Alpha status, this note should
be removed.

# What is this?
This repository includes tools for transforming [OpenMRS](openmrs.org) data
into a FHIR based warehouse. There are two aspects to this transformation:
- **Bulk upload** (a.k.a. _batch mode_): This is used for reading the whole
content of OpenMRS MySQL database, transform it into FHIR resources, and upload
to the target data warehouse.
- **Streaming mode**: In this mode, there is an intermediate binary that
continuously listens to changes in OpenMRS to translate new changes into FHIR
resources and upload them to the target data warehouse.

## Batch mode 
TODO

## Streaming mode
This is currently implemented as a stand alone app that sits between OpenMRS and
the data warehouse. It currently depends on the [Atom Feed module of OpenMRS](
https://wiki.openmrs.org/display/docs/Atom+Feed+Module). Note that this may
change in near future by using [MySQL binlog and Debezium](
https://debezium.io/documentation/reference/1.2/connectors/mysql.html).

The source of data is OpenMRS and the only sink currently implemented is
[GCP FHIR store](https://cloud.google.com/healthcare/docs/concepts/fhir) and
[BigQuery](https://cloud.google.com/bigquery) but it should be easy to add
other FHIR based sinks.

The steps for using this tool are:
- Add Atom Feed module to OpenMRS.
- Add FHIR2 module to OpenMRS and update Atom Feed config.
- Set up the Atom Feed client side database.
- Create the sink FHIR store and BigQuery dataset.
- Compile and run the streaming app.

### Add Atom Feed module to OpenMRS
Assuming that you are using the Reference App of OpenMRS (for example, installed
at `http://localhost:9016`), after login, go to
"System Administration" > "Manage Modules" > "Search from Addons" and search
for the Atom Feed module (e.g., install version 1.0.12).

For any changes in the OpenMRS database, this module creates entries with
payloads related to that change, including URLs for FHIR resources if the
change has corresponding FHIR resources.

### Add FHIR2 module to OpenMRS and update Atom Feed config
The [FHIR module in OpenMRS](
https://wiki.openmrs.org/display/projects/OpenMRS+FHIR+Module) is being
reimplemented in the `fhir2` module. You need to compile this module from
[source](https://github.com/openmrs/openmrs-module-fhir2) and install the
`omod/target/fhir2-1.0.0-SNAPSHOT.omod` module in OpenMRS (or copy that file to
the `modules` directory of your OpenMRS installation).

The URLs for FHIR resources of this module have the form `/ws/fhir2/R4/RESOURCE`
e.g., `http://localhost:9016/openmrs/ws/fhir2/R4/Patient`. Therefor we need to
update the Atom Feed module config to produce these URLs. To do this, from the
OpenMRS Ref. App home page, choose the "Atomfeed" option (this should appear
once the Atom Feed module is installed) and click on "Load Configuration". From
the top menu, choose the file provided in this repository at
[`utils/fhir2_atom_feed_config.json`](utils/fhir2_atom_feed_config.json) and
click "Import".

To check the above two steps, create a new Patient (or an Observation) and
verify that you can see that Patient (or Observation) at the aforementioned
FHIR URL. Then check the Atom Feed URL corresponding to your change, e.g.,

`http://localhost:9016/openmrs/ws/atomfeed/patient/1`

OR

`http://localhost:9016/openmrs/ws/atomfeed/observation/1`

### Set up the Atom Feed client side database
Assuming that you have MySQL running on the default port 3306, simply run:

`mysql --user=USER --password=PASSWORD < utils/create_db.sql`

This will create a database called `atomfeed_client` with required tables (the
`USER` should have permission to create databases). If you want to change the
default database name `atomfeed_client`, you can edit [`utils/create_db.sql`](
utils/create_db.sql) but then you need to change the database name in
[`src/main/resources/hibernate.default.properties`](
src/main/resources/hibernate.default.properties) accordingly.

### Create the sink FHIR store and BigQuery dataset
First you need to create a GCP project. Then create a Google
Cloud Healthcare dataset and a BigQuery dataset with the same name in that
project (for an overview of projects, datasets and data stores check [this](
https://cloud.google.com/healthcare/docs/concepts/projects-datasets-data-stores)
document). Once that is done, use the script
[`utils/create_fhir_store.sh`](utils/create_fhir_store.sh) to create
a FHIR store in this dataset which stream the changes to the BigQuery dataset
as well:

`./utils/create_fhir_store.sh PROJECT LOCATION DATASET FHIR-STORE-NAME`
- `PROJECT` is your GCP project.
- `LOCATION` is GCP location where your dataset resides, e.g., `us-central1`.
- `DATASET` is the name of the dataset you created.
- `FHIR-STORE-NAME` is what it says.

You can run the script with no arguments to see a sample usage.

### Compile and run the streaming app
From the root of your git repo, run:

`mvn compile exec:java -Dexec.mainClass=org.openmrs.FhirStreaming -Dexec.args="http://localhost:9016/openmrs JSESSIONID"`

`JSESSIONID` is the value of a browser cookie with the same name that is
created by OpenMRS. After logging into your OpenMRS instance (e.g.,
`http://localhost:9016/openmrs` in this case), grab the value of `JSESSIONID`
cookie to pass to `FhirStreaming` binary. It is a hexadecimal string like:
`512F6DC48352022DBB8916CCB999B8A5`.

To test your changes, create a new patient (or observation) in OpenMRS and check
that a corresponding Patient (or Observation) FHIR resource is created in the
GCP FHIR store and corresponding rows added to the BigQuery tables.
