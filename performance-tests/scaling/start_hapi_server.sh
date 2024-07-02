set -e # Fail on errors.
set -x # Show each command.

cd ~/gits/hapi-fhir-jpaserver-starter
export PATH=$PATH:~/Downloads/apache-maven-3.9.6/bin
mvn spring-boot:run -Pboot,cloudsql-postgres
