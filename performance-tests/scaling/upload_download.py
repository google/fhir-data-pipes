import json
import os
import time
import datetime

FHIR_SERVER_URL = "http://localhost:8080/fhir"
ENABLE_UPLOAD = os.environ['ENABLE_UPLOAD'] == 'true'
ENABLE_DOWNLOAD = os.environ['ENABLE_DOWNLOAD'] == 'true'
TMP_DIR = "/tmp/scaling"
SOURCE = f"{os.environ['PATIENTS']}_patients"
FHIR_UPLOADER_CORES = os.environ['FHIR_UPLOADER_CORES']
DB_TYPE = os.environ['DB_TYPE']
DIR_WITH_THIS_SCRIPT = os.environ['DIR_WITH_THIS_SCRIPT']
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PATIENTS = os.environ["DB_PATIENTS"]
SQL_ZONE = os.environ["SQL_ZONE"]
PROJECT_ID = os.environ["PROJECT_ID"]
POSTGRES_DB_INSTANCE = os.environ["POSTGRES_DB_INSTANCE"]
JDBC_MODE = os.environ["JDBC_MODE"] == 'true'
FHIR_ETL_RUNNER = os.environ["FHIR_ETL_RUNNER"]
NUM_WORKERS = os.environ["NUM_WORKERS"]

def main():
    shell(f"mkdir -p {TMP_DIR}")
    if ENABLE_UPLOAD:
        upload()
    if ENABLE_DOWNLOAD:
        download()


def download():
    formatted_datetime = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    parquet_subdir = f"parquet_{formatted_datetime}_{SOURCE}_{FHIR_ETL_RUNNER}_jdbcMode{JDBC_MODE}"
    local_parquet_dir = f"{TMP_DIR}/{parquet_subdir}"
    if FHIR_ETL_RUNNER == "DataflowRunner":
        parquet_dir = f"gs://fhir-analytics-test/{parquet_subdir}"
        run_fhir_etl(parquet_dir)
        shell(f'gsutil -m cp -r {parquet_dir} {TMP_DIR}')
    else:
        run_fhir_etl(local_parquet_dir)
    for resource in ["Patient", "Encounter", "Observation"]:
        shell(f"java -jar ./e2e-tests/controller-spark/parquet-tools-1.11.1.jar rowcount {local_parquet_dir}/{resource}/")


def run_fhir_etl(parquet_dir):
    common_etl_args = [
        "--fasterCopy=true",
        f"--runner={FHIR_ETL_RUNNER}",
        f"--resourceList=Patient,Encounter,Observation",
        f"--outputParquetPath={parquet_dir}",
        # Dataflow runner:
        f"--region={SQL_ZONE}",
        f"--numWorkers={NUM_WORKERS}",
        "--gcpTempLocation=gs://fhir-analytics-test/dataflow_temp"
    ]

    if not JDBC_MODE:
        # Test HAPI server readiness.
        shell_run_until_succeeds(
            f"""curl -H "Content-Type: application/json; charset=utf-8" '{FHIR_SERVER_URL}/Patient' -v""")
        shell_measure(
            description=f"Run FhirEtl for {SOURCE} search mode",
            command=" ".join(["java -cp ./pipelines/batch/target/batch-bundled.jar",
                              "com.google.fhir.analytics.FhirEtl",
                              f"--fhirServerUrl={FHIR_SERVER_URL}"] + common_etl_args)
        )

    if JDBC_MODE:
        config_path = os.path.join(TMP_DIR, "hapi-postgres-config.json")
        if DB_TYPE == "postgres":
            json_config = {
                "jdbcDriverClass" : "org.postgresql.Driver",
                "databaseUser" : DB_USERNAME,
                "databasePassword" : DB_PASSWORD,
                "jdbcUri": f"jdbc:postgresql:///{DB_PATIENTS}?cloudSqlInstance={PROJECT_ID}:{SQL_ZONE}:{POSTGRES_DB_INSTANCE}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"
            }
        elif DB_TYPE == "alloy":
            json_config = {
                "jdbcDriverClass" : "org.postgresql.Driver",
                "databaseService" : "postgresql",
                "databaseHostName" : "127.0.0.1",
                "databasePort" : "5432",
                "databaseUser" : DB_USERNAME,
                "databasePassword" : DB_PASSWORD,
                "databaseName" : DB_PATIENTS
            }
        else:
            raise Exception(f"Unknown DB type {DB_TYPE}")
        with open(config_path, "w") as f:
            f.write(json.dumps(json_config, indent=4))
        shell_measure(
            description=f"Run FhirEtl for {SOURCE} JDBC mode",
            command=" ".join(["java -Xmx64g -cp ./pipelines/batch/target/batch-bundled.jar",
                              "com.google.fhir.analytics.FhirEtl",
                              "--jdbcModeHapi=true",
                              f"--fhirDatabaseConfigPath={config_path}"] + common_etl_args)
        )


def upload():
    input_dir = os.path.join(TMP_DIR, SOURCE)
    if not os.path.exists(input_dir):
        shell_measure(
            description=f"Downloaded {SOURCE} from cloud storage",
            command=f'gsutil -m cp -r "gs://synthea-hiv/{SOURCE}" {TMP_DIR}'
        )
        # Test HAPI server readiness.
    shell_run_until_succeeds(
        f"""curl -H "Content-Type: application/json; charset=utf-8" '{FHIR_SERVER_URL}/Patient' -v""")

    # Re-create the database.
    shell_measure(
        description=f"Upload {SOURCE} to HAPI FHIR server; {FHIR_UPLOADER_CORES} cores; {DB_TYPE}",
        command=f"python3 synthea-hiv/uploader/main.py HAPI {FHIR_SERVER_URL} --input_dir {input_dir} --cores {FHIR_UPLOADER_CORES}"
    )


def log(description, start):
    end = time.time()
    full_log = f"{start}\t{description}\t{end - start}"
    shell(f"""echo "{full_log}" >> {TMP_DIR}/upload_download_log.tsv""")


def shell_measure(description, command):
    start = time.time()
    shell(command)
    log(description + "; " + command.replace('\n', ' '), start)


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
