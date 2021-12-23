# OCL-Loaded MySQL Image Setup

>**Note**: The need to create an OCL-loaded MySQL image is infrequent; this is
    only needed for updating OpenMRS release or the OCL dictionary.

The official MySQL image stores data in a Docker volume. This means that if we
load the OCL Dictionary, it is stored in a volume, and not the container, which
means that every time we spin up the MySQL image, we would need to wait 45
minutes for the OCL Dictionary to load.  

We can avoid this long wait time by creating a custom MySQL image that stores
data in the container itself. Once we load the OCL dictionary, we can save the
state of the container using `docker commit`, and tag the newly created image as
`openmrs-fhir-mysql-ocl:latest`. The tagged image is  used in
[openmrs-compose.yaml](../openmrs-compose.yaml), and this directory has the
files needed to create that image.

## Creating OCL Loaded MySQL

Bring up the [`mysql-init-compose.yaml`](./mysql-init-compose.yaml) file:

```bash
docker-compose -f mysql-init-compose.yaml up -d --remove-orphans
```

This will load the OCL dictionary into the MySQL container . This will take
approximately 45 minutes. This step is complete when the OpenMRS login page
loads.

Once the login page loads, commit the MySQL docker image, and tag it. Example:

```bash
docker commit openmrs-fhir-mysql-empty us-docker.pkg.dev/cloud-build-fhir/fhir-analytics/openmrs-fhir-mysql-ocl-small:latest
```

This will generate a MySQL image with the dictionary data loaded into it. You
can push this image to your repo, and then refer to the image in
[openmrs-compose.yaml](../openmrs-compose.yaml). Doing this now allows you to
bring up an OpenMRS image, along with a SQL image loaded with the OCL concepts
without having to wait 45 minutes.
