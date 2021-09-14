# MySQL Setup

The official MySQL image stores data in a Docker volume. This means that if we load the OCL Dictionary, it is stored in a volume, and not the image; this means that every time we spin up the MySQL image, we would need to wait 45 minutes for the OCL Dictionary to load.  

We can avoid this long wait time by creating a custom MySQL image that stores data in the container itself. Once we load the OCL dictionary, we can save the state of the container using `docker commit`, and tag the newly created image as `openmrs-fhir-mysql:ocl-small`. The tagged image is  specifed in [openmrs-compose.yaml](../openmrs-compose.yaml), and this directory has the files needed to create that image.

## Prerequisite

Build the OpenMRS image, with the OCL dictionary loaded into it. The instructions to do so are done in the [openmrs-build](../openmrs-ocl-build) directory.

## Creating OCL Loaded MySQL

First, build the image in the Dockerfile. This is an empty MySQL image, with the [`dbdump`](./dbdump) directory and the config file added. Example:

```bash
docker build -t gobbledegook/openmrs-fhir-mysql:empty .
```

Next, bring up the [`mysql-init-compose.yaml`](./mysql-init-compose.yaml) file:

```bash
docker-compose -f mysql-init-compose.yaml up -d --remove-orphans
```

This will load the OpenMRS webserver with the OCL dictionary, sinking the dictionary content into the custom MySQL container . This will take approximately 45 minutes. This step is complete when the OpenMRS login page loads.

Once the login page loads, commit the MySQL docker image, and tag it. Example:

```bash
docker commit openmrs-fhir-mysql-empty gobbledegook/openmrs-fhir-mysql:ocl-small
```

This will generate a MySQL image with the dictionary data loaded into it. You can push this image to your repo, and then refer to the image in [openmrs-compose.yaml](../openmrs-compose.yaml). Doing this now allows you to bring up an empty OpenMRS image, along with a pre-loaded SQL image without having to wait 45 minutes.
