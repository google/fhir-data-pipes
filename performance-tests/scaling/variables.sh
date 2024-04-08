# 79,768,7885,79370,791562
export PATIENTS=79
export DB_TYPE="alloy"
export RUNNING_ON_HAPI_VM=false
export FHIR_UPLOADER_CORES=16

export DB_PATIENTS="patients_$PATIENTS"
export VM_INSTANCE="pipeline-scaling-20240318-090525"
export VM_ZONE="us-central1-a"
export SQL_ZONE="us-central1"
export PROJECT_ID="fhir-analytics-test"
export PATH="/google/data/ro/projects/java-platform/linux-amd64/jdk-17-latest/bin:$PATH"
export DIR_WITH_THIS_SCRIPT
DIR_WITH_THIS_SCRIPT="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 && pwd -P )"

if [ "$RUNNING_ON_HAPI_VM" = true ]; then
  export RUN_ON_HAPI_STANZA=""
else
  export RUN_ON_HAPI_STANZA=(gcloud compute ssh "$VM_INSTANCE" --zone "$VM_ZONE" --project "$PROJECT_ID" -- -o ProxyCommand='corp-ssh-helper %h %p')
fi

case "$DB_TYPE" in
  "alloy")
    export DB_USERNAME="postgres"
    export DB_PASSWORD="C%_/\Rn-=fI5f$}7"
    ;;
  "postgres")
    export DB_INSTANCE="pipeline-scaling-1"
    ;;
  *)
    echo "Invalid DB_TYPE $DB_TYPE"
    ;;
esac


