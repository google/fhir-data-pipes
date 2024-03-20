VM_INSTANCE="pipeline-scaling-20240318-090525"
VM_ZONE="us-central1-a"
PROJECT_ID="fhir-analytics-test"

killall /usr/bin/ssh || true
gcloud compute ssh $VM_INSTANCE --zone $VM_ZONE --project $PROJECT_ID -- -o ProxyCommand='corp-ssh-helper %h %p' -NL 8080:localhost:8080 &
