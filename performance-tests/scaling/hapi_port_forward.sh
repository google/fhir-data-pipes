source ./variables.sh
set -o nounset
killall /usr/bin/ssh || true
gcloud compute ssh $VM_INSTANCE --zone $VM_ZONE --project $PROJECT_ID -- -o ProxyCommand='corp-ssh-helper %h %p' -NL 8080:localhost:8080 &
