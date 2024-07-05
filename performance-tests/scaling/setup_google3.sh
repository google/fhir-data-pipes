source ./variables.sh

set -e # Fail on errors.
set -x # Show each command.
set -o nounset

# Kill the current HAPI server to allow to delete and set the database.
"${RUN_ON_HAPI_STANZA[@]}" "sudo killall /usr/bin/java || true"
"${RUN_ON_HAPI_STANZA[@]}" "sudo killall python || true"
"${RUN_ON_HAPI_STANZA[@]}" "sudo killall python3 || true"
"${RUN_ON_HAPI_STANZA[@]}" "sudo killall sh || true"

case "$DB_TYPE" in
  "alloy")
    ALLOY_INSTANCE="projects/fhir-analytics-test/locations/us-central1/clusters/pipeline-scaling-alloydb-1/instances/pipeline-scaling-alloydb-largest"
    sudo killall alloydb-auth-proxy || true
    nohup ~/Downloads/alloydb-auth-proxy $ALLOY_INSTANCE &
    sleep 1
    if [[ "$ENABLE_UPLOAD" = true ]]; then
      for cmd in "DROP DATABASE IF EXISTS" "CREATE DATABASE"; do
        PGPASSWORD="$DB_PASSWORD" psql -h 127.0.0.1 -p 5432 -U "$DB_USERNAME" -c "$cmd $DB_PATIENTS"
      done
    else
      # Check DB connection.
      PGPASSWORD="$DB_PASSWORD" psql -h 127.0.0.1 -p 5432 -U "$DB_USERNAME" -c "SELECT 1"
    fi
    DB_CONNECTION="jdbc:postgresql:///${DB_PATIENTS}?127.0.0.1:5432"
    ;;
  "postgres")
    if [[ "$ENABLE_UPLOAD" = true ]]; then
      gcloud sql databases delete "$DB_PATIENTS" --instance="$POSTGRES_DB_INSTANCE" --quiet || true
      gcloud sql databases create "$DB_PATIENTS" --instance="$POSTGRES_DB_INSTANCE"
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
"${RUN_ON_HAPI_STANZA[@]}" "sed -i '/    username: .*/c\\    username: ${DB_USERNAME}' $APPLICATION_YAML"
# Turn off search index because we don't use it and it might conflict between load-balanced HAPI servers.
# Reference: https://hapifhir.io/hapi-fhir/docs/server_jpa/elastic.html
"${RUN_ON_HAPI_STANZA[@]}" "sed -i '/    hibernate.search.enabled: true/c\\    hibernate.search.enabled: false' $APPLICATION_YAML"
# Start the HAPI server.
# shellcheck disable=SC2088
nohup "${RUN_ON_HAPI_STANZA[@]}" "~/gits/fhir-data-pipes/performance-tests/scaling/start_hapi_server.sh" >> "$TMP_DIR/nohup-hapi-$(date +%Y-%m-%d).out" 2>&1 &

if [ "$RUNNING_ON_HAPI_VM" = false ]; then
  (sleep 15; "$DIR_WITH_THIS_SCRIPT/hapi_port_forward.sh") &
fi

# tail -F ~/nohup-hapi.out
