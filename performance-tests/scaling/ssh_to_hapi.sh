source ./variables.sh

set -e # Fail on errors.
set -x # Show each command.

gcloud compute ssh $VM_INSTANCE --zone $VM_ZONE --project $PROJECT_ID -- -o ProxyCommand='corp-ssh-helper %h %p'