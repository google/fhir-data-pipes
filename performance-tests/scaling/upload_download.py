import os
import time

FHIR_SERVER_URL = "http://localhost:8080/fhir"
ENABLE_UPLOAD = os.environ['ENABLE_UPLOAD'] == 'true'
ENABLE_DOWNLOAD = os.environ['ENABLE_DOWNLOAD'] == 'true'
TMP_DIR = "/tmp/scaling"
SOURCE = f"{os.environ['PATIENTS']}_patients"
FHIR_UPLOADER_CORES = os.environ['FHIR_UPLOADER_CORES']
DB_TYPE = os.environ['DB_TYPE']
DIR_WITH_THIS_SCRIPT = os.environ['DIR_WITH_THIS_SCRIPT']

def main():
    shell(f"mkdir -p {TMP_DIR}")

    input_dir = os.path.join(TMP_DIR, SOURCE)
    parquet_dir = os.path.join(TMP_DIR, f"parquet_{SOURCE}")

    if ENABLE_UPLOAD and not os.path.exists(input_dir):
        shell_measure(
            description=f"Downloaded {SOURCE} from cloud storage",
            command=f'gsutil -m cp -r "gs://synthea-hiv/{SOURCE}" {TMP_DIR}'
        )

    shell(f"rm -rf {parquet_dir}")

    # Test HAPI server readiness.
    shell_run_until_succeeds(
        f"""curl -H "Content-Type: application/json; charset=utf-8" '{FHIR_SERVER_URL}/Patient' -v""")

    if ENABLE_UPLOAD:
        # Re-create the database.
        shell_measure(
            description=f"Upload {SOURCE} to HAPI FHIR server; {FHIR_UPLOADER_CORES} cores; {DB_TYPE}",
            command=f"python3 synthea-hiv/uploader/main.py HAPI {FHIR_SERVER_URL} --input_dir {input_dir} --cores {FHIR_UPLOADER_CORES}"
        )
    if False:
        shell_measure(
            description=f"Run FhirEtl for {SOURCE}",
            command=" ".join(["java -cp ./pipelines/batch/target/batch-bundled.jar",
                              "com.google.fhir.analytics.FhirEtl",
                              f"--fhirServerUrl={FHIR_SERVER_URL}",
                              f"--outputParquetPath={parquet_dir}"])
        )
    if ENABLE_DOWNLOAD:
        shell_measure(
            description=f"Run FhirEtl for {SOURCE} JDBC mode",
            command=" ".join(["java -Xmx128g -cp ./pipelines/batch/target/batch-bundled.jar",
                              "com.google.fhir.analytics.FhirEtl",
                              "--jdbcModeHapi=true",
                              f"--fhirDatabaseConfigPath={DIR_WITH_THIS_SCRIPT}/../../utils/hapi-postgres-config.json",
                              f"--outputParquetPath={parquet_dir}"])
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
