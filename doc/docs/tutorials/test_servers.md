# Set-up Local Test Servers

This guide shows how to use provided Docker images to bring up test servers (and
optionally load with synthetic data) to easily get started the FHIR Data Pipes
pipelines.

There are _3 Docker server_ configurations you can use for testing:

- HAPI FHIR server with Postgres (source)
- OpenMRS Reference Application with MySQL (source)
- HAPI FHIR server (destination)

## Instructions

**Note**: All commands are run from the root directory of the repository.

1.  Create an external Docker network named `cloudbuild`:

    ```
    docker network create cloudbuild
    ```

2.  Bring up a FHIR source server for the pipeline. This can be either:

    - [HAPI FHIR server with Postgres](https://github.com/google/fhir-data-pipes/blob/master/docker/hapi-compose.yml):

      ```shell
      docker-compose  -f ./docker/hapi-compose.yml up  --force-recreate -d
      ```

      The base FHIR URL for this server is `http://localhost:8091/fhir`. If you
      get a CORS error when accessing the URL, try manually refreshing (e.g.
      ctrl-shift-r).

    - [OpenMRS Reference Application with MySQL](https://github.com/google/fhir-data-pipes/blob/master/docker/openmrs-compose.yaml):

      ```shell
      docker-compose -f ./docker/openmrs-compose.yml up --force-recreate -d
      ```

      The base FHIR URL for this server is
      `http://localhost:8099/openmrs/ws/fhir2/R4`

3.  Upload the synthetic data stored in
    [sample_data](https://github.com/google/fhir-data-pipes/blob/master/synthea-hiv/sample_data)
    to the FHIR server that you brought up using the
    [Synthea data uploader](https://github.com/google/fhir-data-pipes/blob/master/synthea-hiv/README.md#Uploader).

    The uploader requires that you install the `uploader` module requirements first. You can do this by running:

    ```shell
    pip3 install -r ./synthea-hiv/uploader/requirements.txt
    ```
    Please note, it is a good idea to first create a Python `virtualenv` before running the above command to
    avoid conflicts with other Python packages you may have installed globally. You can do this by running:
    ```shell
    $ virtualenv -p python3.8 venv
    $ . ./venv/bin/activate
    ```

    Then, you can run the uploader script to upload the synthetic data to the FHIR server.

    For example, to upload to the HAPI FHIR server brought up in the previous
    step, run:

    ```shell
    python3 ./synthea-hiv/uploader/main.py HAPI http://localhost:8091/fhir \
    --input_dir ./synthea-hiv/sample_data --cores 8
    ```

    Depending on your machine, using too many cores may slow down your machine
    or cause JDBC connection pool errors with the HAPI FHIR server. Reducing the
    number of cores using the `--cores` flag should help at the cost of
    increasing the time to upload the data.

4.  (optional) If you only want to output Apache Parquet files, there is no
    additional setup. If you want to test outputting to another FHIR server,
    then bring up a destination
    [HAPI FHIR server](https://github.com/google/fhir-data-pipes/blob/master/docker/sink-compose.yml):

    ```shell
    docker-compose  -f ./docker/sink-compose.yml up  --force-recreate -d
    ```

    The base URL for this server is `http://localhost:8098/fhir`.

## Additional notes for OpenMRS

Once running you can access OpenMRS at <http://localhost:8099/openmrs/> using
username "admin" and password "Admin123". The Docker image includes the required
FHIR2 module and demo data. Edit `docker/openmrs-compose.yaml` to change the
default port.

**Note:** If `docker-compose` fails, you may need to adjust file permissions. In
particular if the permissions on `mysqld.cnf` is not right, the `datadir` set in
this file will not be read by MySQL and it will cause OpenMRS to require its
`initialsetup` (which is not needed since the MySQL image already has all the
data and tables needed):

```shell
$ docker-compose -f docker/openmrs-compose.yaml down -v
$ chmod a+r docker/mysql-build/mysqld.cnf
$ chmod -R a+r ./utils
$ docker-compose -f docker/openmrs-compose.yaml up
```

In order to see the demo data in OpenMRS you must rebuild the search index. In
OpenMRS go to **Home > System Administration > Advanced Administration**. Under
**Maintenance** go to **Search Index** then **Rebuild Search Index**.
