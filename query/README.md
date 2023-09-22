# Query FHIR Data and Views
This directory contains our new (under development) approach for querying
transformed FHIR data which is replacing the [old approach](../dwh). The new
approach is based on
[FHIR-views](https://github.com/google/fhir-py/tree/main/google-fhir-views)
and eventually
[SQL-on-FHIR v2 spec](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/).
The idea is to create flat views for FHIR resources which are easy to work with.

When the data is transformed into Analytics friendly formats (e.g., Parquet)
using the [pipelines](../pipelines), it resembles the nested and repeated
structure of FHIR resources. It is hard to write SQL queries for these
structures. With
[FHIR-views](https://github.com/google/fhir-py/tree/main/google-fhir-views)
we can define custom flat views using
[FHIRPath](https://hl7.org/fhir/fhirpath.html) statements.

To see examples of this approach, see
[using_fhir_views.ipynb](using_fhir_views.ipynb) which also has some direct SQL
query examples. The defaults in this notebook assume that you are running a
local Spark Thrift server container as described
[here](https://github.com/google/fhir-data-pipes/wiki/Analytics-on-a-single-machine-using-Docker#run-the-single-machine-configuration).
You can also create a docker image to easily experiment with FHIR-views by:
```shell
docker build -t my-fhir-views .
```
and then:
```shell
docker run -p 10002:8888 --network cloudbuild my-fhir-views
```
The `--network` part is to make it easy to communicate with the sample
Thrift server mentioned above. If you want to connect to another Thrift server,
you don't need to use that network.


After the container starts, copy the token printed on the screen, connect to
`http://localhost:10002/lab?token=TOKEN`
and start experimenting. [using_fhir_views.ipynb](using_fhir_views.ipynb) is
also copied into this image which can be used as a starting point.
