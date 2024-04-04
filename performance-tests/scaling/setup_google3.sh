source ./variables.sh

set -e # Fail on errors.
set -x # Show each command.
set -o nounset

# Kill the current HAPI server to allow to delete the database.
gcloud compute ssh $VM_INSTANCE --zone $VM_ZONE --project $PROJECT_ID -- -o ProxyCommand='corp-ssh-helper %h %p' "sudo killall /usr/bin/java || true"

gcloud sql databases delete "$PATIENTS" --instance="$DB_INSTANCE" --quiet || true
gcloud sql databases create "$PATIENTS" --instance="$DB_INSTANCE"

DB_CONNECTION="    url: jdbc:postgresql:///${PATIENTS}?cloudSqlInstance=${PROJECT_ID}:${SQL_ZONE}:${DB_INSTANCE}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"

# Kill the server and update the DB connection config.
HAPI_VM_COMMANDS="
cd ~/gits/hapi-fhir-jpaserver-starter
sed -i '/.*url: jdbc:postgresql:.*/c\\${DB_CONNECTION}' src/main/resources/application.yaml
~/start_hapi_server.sh"

gcloud compute ssh $VM_INSTANCE --zone $VM_ZONE --project $PROJECT_ID -- -o ProxyCommand='corp-ssh-helper %h %p' "$HAPI_VM_COMMANDS"

sleep 30
~/hapi_port_forward.sh