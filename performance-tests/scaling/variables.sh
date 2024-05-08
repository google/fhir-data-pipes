[[ -f "config.sh" ]] || cp config.sh.tmpl config.sh

source ./config.sh

export PATH="/google/data/ro/projects/java-platform/linux-amd64/jdk-17-latest/bin:$PATH"
export DB_PATIENTS="patients_$PATIENTS"
export DIR_WITH_THIS_SCRIPT
DIR_WITH_THIS_SCRIPT="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 && pwd -P )"

case "$POSTGRES_DB_INSTANCE" in
  "pipeline-scaling-belgium")
    export DB_USERNAME="postgres"
    export DB_PASSWORD="C%_/\Rn-=fI5f$}7"
    export ZONE=belgium
    ;;
  "pipeline-scaling-1")
    export DB_USERNAME="pipeline-scaling-user"
    export DB_PASSWORD="C%_/\Rn-=fI5f$}7"
    export ZONE=us-central
    ;;
  "pipeline-scaling-2")
    export DB_USERNAME="postgres"
    export DB_PASSWORD="C%_/\Rn-=fI5f$}7"
    export ZONE=us-central
    ;;
  *)
    echo "Invalid POSTGRES_DB_INSTANCE $POSTGRES_DB_INSTANCE"
    exit 2
    ;;
esac

case "$ZONE" in
  "belgium")
    export VM_INSTANCE="pipeline-scaling-20240318-20240413-193428"
    export VM_ZONE="europe-west1-b"
    export SQL_ZONE="europe-west1"
    ;;
  "us-central")
    export VM_INSTANCE=pipeline-scaling-20240508
    # export VM_INSTANCE="pipeline-scaling-20240318-090525"
    export VM_ZONE="us-central1-a"
    export SQL_ZONE="us-central1"
    ;;
  *)
    echo "Invalid ZONE $ZONE"
    exit 1
    ;;
esac

if [ "$RUNNING_ON_HAPI_VM" = true ]; then
  export RUN_ON_HAPI_STANZA=(ssh localhost)
else
  export RUN_ON_HAPI_STANZA=(gcloud compute ssh "$VM_INSTANCE" --zone "$VM_ZONE" --project "$PROJECT_ID" -- -o ProxyCommand='corp-ssh-helper %h %p')
fi
