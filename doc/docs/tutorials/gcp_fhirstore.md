# Create a Google Cloud FHIR Store and BigQuery Dataset

One of the supported FHIR sinks is
[Google Cloud FHIR store](https://cloud.google.com/healthcare/docs/concepts/fhir)
and [BigQuery](https://cloud.google.com/bigquery). These instructions will guide
you in creating a FHIR store and BigQuery dataset.

To set up GCP project that you can use as a sink FHIR store:

-   Create a new project in [GCP](https://console.cloud.google.com). For an
    overview of projects, datasets and data stores check
    [this document](https://cloud.google.com/healthcare/docs/concepts/projects-datasets-data-stores).
-   Enable Google Cloud Healthcare API in the project and create a Google Cloud
    Healthcare dataset
-   Create a FHIR data store in the dataset with the R4 FHIR version
-   Enable the BigQuery API in the project and dataset with the same name in the
    project
-   Download, install, and initialize the `gcloud` cli:
    https://cloud.google.com/sdk/docs/quickstart
-   Make sure you can authenticate with the project using the CLI:
    https://developers.google.com/identity/sign-in/web/sign-in
    *   `$ gcloud init`
    *   `$ gcloud auth application-default login`
    *   Create a service account for the project, generate a key, and save it
        securely locally
    *   Add the `bigquery.dataEditor` and `bigquery.jobUser` roles to the
        project in the `IAM & Admin`/`Roles` settings or using the cli:
        -   `$ gcloud projects add-iam-policy-binding openmrs-260803 --role
            roles/bigquery.admin --member
            serviceAccount:openmrs-fhir-analytics@openmrs-260803.iam.gserviceaccount.com`
        -   `$ gcloud projects add-iam-policy-binding openmrs-260803 --role
            roles/healthcare.datasetAdmin --member
            serviceAccount:openmrs-fhir-analytics@openmrs-260803.iam.gserviceaccount.com`
    *   Activate the service account for your project using `gcloud auth
        activate-service-account <your-service-account>
        --key-file=<your-key-file> --project=<your project>`
    *   Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable:
        https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable
-   Use the script [`utils/create_fhir_store.sh`](utils/create_fhir_store.sh) to
    create a FHIR store in this dataset which stream the changes to the BigQuery
    dataset as well: `./utils/create_fhir_store.sh PROJECT LOCATION DATASET
    FHIR-STORE-NAME`

    -   `PROJECT` is your GCP project.
    -   `LOCATION` is GCP location where your dataset resides, e.g.,
        `us-central1`.
    -   `DATASET` is the name of the dataset you created.
    -   `FHIR-STORE-NAME` is what it says.

    *Note: If you get `PERMISSION_DENIED` errors, make sure to `IAM &
    ADMIN`/`IAM`/`Members` and add the `bigquery.dataEditor` and
    `bigquery.jobUser` roles to the `Cloud Healthcare Service Agent` service
    account that shows up.*

You can run the script with no arguments to see a sample usage. After you create
the FHIR store, its full URL would be:

`https://healthcare.googleapis.com/v1/projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIR-STORE-NAME`
