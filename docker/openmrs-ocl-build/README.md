# openmrs-docker-build

Repo to take OpenMRS files and generate two Docker Images: an empty image, and one with the OCL Dictionary

## Generate Docker Files

```bash
mvn org.openmrs.maven.plugins:openmrs-sdk-maven-plugin:setup-sdk
mvn openmrs-sdk:build-distro -Ddistro=./openmrs-distro.properties
```

## Build and Push Empty Docker Image

First build the image. For example:

```bash
docker build -t gobbledegook/openmrs-reference-application-distro:empty docker/web/.
```

Then push it:

```bash
docker push gobbledegook/openmrs-reference-application-distro:empty 
```

This image is used in the [`openmrs-compose.yaml`](../openmrs-compose.yaml) config.

## Build and Push the OCL-loaded Docker Image

First, copy the zip file, `20210827_140605.zip`, to `docker/web`. This ZIP file contains the [CIEL OCL Dictionary](https://app.openconceptlab.org/#/orgs/CIEL/sources/CIEL) donwloaded on August 27, 2021. The server will not startup until the import is completed. Startup will take ~45 minutes.

Then, in the `Dockerfile` in `docker/web`, add the statement below prior to the last line:

```Dockerfile
COPY 20210827_140605.zip /usr/local/tomcat/.OpenMRS/ocl/configuration/loadAtStartup/20210827_140605.zip
RUN apt-get clean && apt-get update && apt-get install -y dialog apt-utils
RUN apt-get install -y locales
RUN locale-gen
```

Next, build the image. For example:

```bash
docker build -t gobbledegook/openmrs-reference-application-distro:ocl-load docker/web/.
```

Then push it:

```bash
docker push gobbledegook/openmrs-reference-application-distro:ocl-load 
```

This image is used in the [`mysql-init-compose.yaml`](../mysql-build/mysql-init-compose.yaml) config.