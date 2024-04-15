source ./variables.sh

set -e # Fail on errors.
set -x # Show each command.
set -o nounset

# Kill the current HAPI server to allow to delete and set the database.
"${RUN_ON_HAPI_STANZA[@]}" "sudo killall /usr/bin/java || true"

case "$DB_TYPE" in
  "alloy")
    if [ "$RUNNING_ON_HAPI_VM" = false ]; then
      ALLOY_INSTANCE="projects/fhir-analytics-test/locations/us-central1/clusters/pipeline-scaling-alloydb-1/instances/pipeline-scaling-alloydb-largest"
      nohup ~/Downloads/alloydb-auth-proxy $ALLOY_INSTANCE &
    fi
    if [[ "$ENABLE_UPLOAD" = true ]]; then
      for cmd in "DROP DATABASE IF EXISTS" "CREATE DATABASE"; do
        PGPASSWORD="$DB_PASSWORD" psql -h 127.0.0.1 -p 5432 -U postgres -c "$cmd $DB_PATIENTS"
      done
    fi
    DB_CONNECTION="jdbc:postgresql:///${DB_PATIENTS}?127.0.0.1:5432"
    ;;
  "postgres")
    if [[ "$ENABLE_UPLOAD" = true ]]; then
      gcloud sql databases delete "$DB_PATIENTS" --instance="$DB_INSTANCE" --quiet || true
      gcloud sql databases create "$DB_PATIENTS" --instance="$DB_INSTANCE"
    fi
    DB_CONNECTION="jdbc:postgresql:///${DB_PATIENTS}?cloudSqlInstance=${PROJECT_ID}:${SQL_ZONE}:${POSTGRES_DB_INSTANCE}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"
    ;;
  *)
    echo "Invalid DB_TYPE $DB_TYPE"
    ;;
esac

# shellcheck disable=SC2088
APPLICATION_YAML="~/gits/hapi-fhir-jpaserver-starter/src/main/resources/application.yaml"

# Update the DB connection config.
"${RUN_ON_HAPI_STANZA[@]}" "sed -i '/.*url: jdbc:postgresql:.*/c\\    url: ${DB_CONNECTION}' $APPLICATION_YAML"
# Start the HAPI server.
# shellcheck disable=SC2088
nohup "${RUN_ON_HAPI_STANZA[@]}" "~/gits/fhir-data-pipes/performance-tests/scaling/start_hapi_server.sh" >> ~/nohup-hapi.out 2>&1 &

if [ "$RUNNING_ON_HAPI_VM" = false ]; then
  sleep 15
  sudo killall /usr/bin/ssh || true
  "$DIR_WITH_THIS_SCRIPT/hapi_port_forward.sh" || true
fi

tail -F ~/nohup-hapi.out
