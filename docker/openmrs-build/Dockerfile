### SETUP OPENMRS BINARIES
FROM adoptopenjdk/maven-openjdk8  AS openmrs-builder

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    wget \
    lsb-release 

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg |  gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

RUN echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" |  tee /etc/apt/sources.list.d/docker.list > /dev/null

RUN  apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io

RUN mvn -e org.openmrs.maven.plugins:openmrs-sdk-maven-plugin:setup-sdk -B

RUN mvn -e openmrs-sdk:setup -DserverId=openmrs-reference-application-distro -Dport=8080 -Ddistro=referenceapplication:2.12.0-SNAPSHOT  -DdbDriver=mysql -DdbReset=true -Ddebug=2023 -DdbUri="jdbc:mysql://localhost:3306/@DBNAME@" -DserverPort=8080 -DdbUser=root -DdbPassword=debezium  -DjavaHome="/opt/java/openjdk/"

WORKDIR  /root/openmrs/openmrs-reference-application-distro
RUN wget https://addons.openmrs.org/api/v1/addon/org.openmrs.module.atomfeed/1.0.13/download -O modules/atomfeed-1.0.13.omod

RUN echo "omod.atomfeed=1.0.13" >>  openmrs-distro.properties 
RUN echo "omod.atomfeed=1.0.13" >>  openmrs-server.properties

RUN mvn -e openmrs-sdk:build-distro -Ddistro=./openmrs-distro.properties


### SETUP EMPTY OPENMRS IMAGE BUILDER
FROM openmrs-builder AS openmrs-empty
WORKDIR /root
COPY --from=openmrs-builder /root/openmrs/openmrs-reference-application-distro/docker/web /root
RUN sed -i '$ d' /root/Dockerfile
RUN echo "RUN apt-get clean && apt-get update && apt-get install -y dialog apt-utils" >> /root/Dockerfile
RUN echo "RUN apt-get clean && apt-get update && apt-get install -y dialog apt-utils" >> /root/Dockerfile
RUN echo "RUN apt-get install -y locales" >> /root/Dockerfile
RUN echo "RUN locale-gen" >> /root/Dockerfile
RUN echo "CMD [\"/usr/local/tomcat/startup.sh\"]" >> /root/Dockerfile

ENV REPO_NAME="googldegook"
ENTRYPOINT  docker build -t ${REPO_NAME}/openmrs-reference-application-distro:empty .


### SETUP OCL LOADED OPENMRS IMAGE BUILDER
FROM openmrs-builder AS openmrs-ocl
WORKDIR /root
COPY --from=openmrs-empty /root /root
COPY 20210827_140605.zip 20210827_140605.zip

RUN sed -i '$ d' /root/Dockerfile
RUN echo "COPY 20210827_140605.zip /usr/local/tomcat/.OpenMRS/ocl/configuration/loadAtStartup/20210827_140605.zip" >> /root/Dockerfile
RUN echo "CMD [\"/usr/local/tomcat/startup.sh\"]" >> /root/Dockerfile

ENV REPO_NAME="googldegook"
ENTRYPOINT  docker build -t ${REPO_NAME}/openmrs-reference-application-distro:ocl-load .




