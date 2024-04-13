set -e # Fail on errors.
set -x # Show each command.

source "./variables.sh"

GITS_DIR=~/gits
cd $GITS_DIR
[[ -d "fhir-data-pipes" ]] || git clone https://github.com/google/fhir-data-pipes.git
cd fhir-data-pipes

chmod -R 755 ./utils
sudo apt-get -y install maven
sudo apt install npm
mvn clean install -P dataflow-runner

sudo apt-get install postgresql-client
wget https://storage.googleapis.com/alloydb-auth-proxy/v1.7.1/alloydb-auth-proxy.linux.amd64 -O ~/Downloads/alloydb-auth-proxy
chmod +x ~/Downloads/alloydb-auth-proxy
