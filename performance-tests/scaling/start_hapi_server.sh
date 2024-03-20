# To be executed on the VM with HAPI server.

cd ~/gits/hapi-fhir-jpaserver-starter
export PATH=$PATH:~/Downloads/apache-maven-3.9.6/bin
nohup mvn spring-boot:run -Pboot &
