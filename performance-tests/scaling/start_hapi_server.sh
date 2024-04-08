set -e # Fail on errors.
set -x # Show each command.

cd ~/gits/hapi-fhir-jpaserver-starter
export PATH=$PATH:~/Downloads/apache-maven-3.9.6/bin
nohup mvn spring-boot:run -Pboot >> ~/nohup-hapi.out 2>&1 &

# source ./variables.sh
# set -o nounset
# gcloud compute ssh $VM_INSTANCE --zone $VM_ZONE --project $PROJECT_ID -- -o ProxyCommand='corp-ssh-helper %h %p' "./start_hapi_server.sh"
