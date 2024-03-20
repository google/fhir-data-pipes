import os
import time

DB_INSTANCE = "pipeline-scaling-1"
SOURCE = "79_patients"
# SOURCE = "7885_patients"
FHIR_SERVER_URL = "http://localhost:8080/fhir"
ENABLE_UPLOAD = True
ENABLE_DOWNLOAD = True
TMP_DIR = "/tmp/scaling"


def main():
    shell(f"mkdir -p {TMP_DIR}")

    if not os.path.exists(SOURCE):
        shell_measure(
            description=f"Downloaded {SOURCE} from cloud storage",
            command=f'gsutil -m cp -r "gs://synthea-hiv/{SOURCE}" {TMP_DIR}'
        )

    # Test HAPI server readiness.
    shell_run_until_succeeds(
        f"""curl -H "Content-Type: application/json; charset=utf-8" '{FHIR_SERVER_URL}/Patient' -v""")

    if ENABLE_UPLOAD:
        # Re-create the database.
        shell(f"gcloud sql databases delete {SOURCE} --instance={DB_INSTANCE}", exit_on_failure=False)
        shell(f"gcloud sql databases create {SOURCE} --instance={DB_INSTANCE}")
        shell_measure(
            description=f"Upload {SOURCE} to HAPI FHIR server",
            command=f"python3 synthea-hiv/uploader/main.py HAPI {FHIR_SERVER_URL} --input_dir {SOURCE}"
        )
    if ENABLE_DOWNLOAD:
        shell_measure(
            description=f"Run FhirEtl for {SOURCE}",
            command=" ".join(["java -cp ./pipelines/batch/target/batch-bundled.jar",
                              "com.google.fhir.analytics.FhirEtl",
                              f"--fhirServerUrl={FHIR_SERVER_URL}",
                              f"--outputParquetPath={TMP_DIR}/parquet_{SOURCE}"])
        )


def log(description, start):
    end = time.time()
    full_log = f"{start}\t{description}\t{end - start}"
    shell(f"""echo "{full_log}" >> {TMP_DIR}/upload_download_log.tsv""")


def shell_measure(description, command):
    start = time.time()
    shell(command)
    log(description, start)


def shell(command, exit_on_failure=True):
    print(command)
    return_code = os.system(command)
    if return_code != 0:
        if exit_on_failure:
            raise Exception(f"Command failed with {return_code}: {command}")
        else:
            print(f"Warning: Command failed with {return_code}: {command}")


def shell_run_until_succeeds(command):
    while True:
        print(command)
        return_code = os.system(command)
        if return_code != 0:
            time.sleep(1)
        else:
            break


main()
