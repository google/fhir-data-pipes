# OpenMRS Image Builder

This directory contains a multi-staged `Dockerfile` to build two OpenMRS images:

* An empty OpenMRS image
* An OpenMRS image with the OCL ZIP file

The empty OpenMRS image is used in the [`openmrs-compose.yaml`](../openmrs-compose.yaml) YAML

The OCL image is used in the [`mysql-init-compose.yaml`](../mysql-build/mysql-init-compose.yaml) to setup the custom SQL image

## PRE-REQ

* MySQL instance running on host machine, listening to port 3306
* Docker running on host machine

## HOW TO RUN

### Empty OpenMRS image builder

This build a Docker image tagged as:
`$REPO_NAME/openmrs-openmrs-reference-application-distro:empty`

```bash
    docker build \
      --target openmrs-empty \
      -t openmrs-builder:empty \
      --network=host .
    
    docker run -it --network=host \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -e REPO_NAME=$REPO_NAME \
      openmrs-builder:empty
```

### OCL loaded OpenMRS image builder

This build a Docker image tagged as:
`$REPO_NAME/openmrs-openmrs-reference-application-distro:ocl-load`

```bash
docker build  \
  -t openmrs-builder:ocl \
  --network=host .
    
docker run -it --network=host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e REPO_NAME=$REPO_NAME \
  openmrs-builder:ocl
```
