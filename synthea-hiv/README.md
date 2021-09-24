# Synthea-HIV

This is an experimental module for generating OpenMRS patients with HIV using
the [Synthea](https://github.com/synthetichealth/synthea) fake data generator,
and then uploading the transaction Bundles to a FHIR Server.

## Generator

The [`generator`](generator) folder has the model for generating patients, as
well as the Dockerfile used to build the `gobbledegook/synthea:latest` image.

To generate data, run the following command in your terminal (Docker is needed):

```bash
docker run  -it   -v "$(pwd)"/output:/output -e POP=100  gobbledegook/synthea:latest
```

Setting `POP` to the number of patients you want to generate.

The [`sample_output`](sample_output) folder contains sample data from running
the generator.

## Uploader

Run the following command in your terminal (Docker is needed):

```bash
gcloud auth application-default login
cd ./uploader
docker build -t uploader .
docker run -it --network=host \
  -e FHIR_ENDPOINT=$FHIR_ENDPOINT \
  -e CONVERT="--convert_to_openmrs" \
  -v ~/.config/:/root/.config \
  -v /absolute/path/to/fhir/bundles:/workspace/output/fhir \
  uploader
```

If you are not uploading to Google Cloud FHIR Store, the command:

  ```bash
  gcloud auth application-default login
  ```

can be omitted, as well as mounting the `.config` directory.

The `$FHIR_ENDPOINT` is the endpoint to upload to.

* For a local OpenMRS endpoint, it is:

    ```bash
    http://localhost:8099/openmrs/ws/fhir2/R4
    ```

* For GCP FHIR Store, the format is:

  ```bash
  https://healthcare.googleapis.com/v1beta1/projects/$PROJECT_ID/locations/$LOCATION/datasets/$DATASET/fhirStores/$FHIR_STORE/fhir
  ```

If the `FHIR_ENDPOINT` env variable is not set, the localhost URL is used by
default.

The `CONVERT` variable is optional, but if you want to upload to an OpenMRS
server, you _must_ add it and set its value to `--convert_to_openmrs` flag. See
the [Limitations](#Limitations) section below as to why.

## Model

The model is based on the
[list](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/179#issuecomment-895040775)
of essential codes (Q&A) that are frequently used by AMPATH.

![alt text](generator/model.png "Simple HIV Workflow")

The main module contains calls to submodules. Each submodule is a question used
by AMPATH, with a distribution of answers that can be chosen. Below is an
example of how a Submodule looks:

![alt text](generator/submodule.png "Simple HIV Workflow")

## Limitations

* Synthea only accepts LOINC and SNOMED-CT as codes and value codes. OpenMRS
  works with CIEL codes. To get around this, even though the JSON files in the
  [`generator`](generator) folder refer to LOINC or SNOMED-CT as the code, the
  coded value of each question and answer is actually from CIEL. This is
  regardless of whether the flag `convert_to_openmrs` is specified or not when
  uploading.

* Some of the questions asked by AMPATH can have multiple answers in real life;
  in the model, each of the questions can have one answer. Each answer is
  assigned a random probabiltiy of being chosen.

* Due to [this](https://github.com/moby/moby/issues/2259) issue in Docker, the
  outputted directories and files generated will be owned by `root`, not your
  user. When deleting the directories/files without `sudo` or admin privileges,
  you will get permission denied.

* The OpenMRS FHIR Module does not yet support uploading Bundles. To circumvent
  this, the uploader splits each Bundle into the indiviual Patient, Encounters,
  and Observations resources before uploading.
