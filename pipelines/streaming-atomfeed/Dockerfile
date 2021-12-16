# docker build -f pipelines/streaming-atomfeed/Dockerfile ./ -t ghcr.io/i-tech-uw/openmrs-fhir-analytics-streaming-atomfeed:latest
FROM adoptopenjdk/maven-openjdk8 as build

WORKDIR /app

# Selective Adds to Speed Up Build
ADD ../pipelines/common ./pipelines/common
ADD ../pipelines/streaming-atomfeed/pom.xml ./pipelines/streaming-atomfeed/pom.xml 
ADD ../pipelines/streaming-binlog/pom.xml ./pipelines/streaming-binlog/pom.xml 
ADD ../pipelines/batch/pom.xml ./pipelines/batch/pom.xml
ADD ../pipelines/pom.xml ./pipelines/pom.xml
ADD ../bunsen ./bunsen
ADD ../pom.xml ./

RUN mvn -B -pl streaming-atomfeed -am verify --fail-never

ADD ../pipelines/streaming-atomfeed/src ./pipelines/streaming-atomfeed/src

RUN mvn -B install -DskipTests -Dlicense.skip=true -pl pipelines/streaming-atomfeed -am -B

RUN /bin/bash -l -c 'echo echo "$(mvn -q help:evaluate -Dexpression=project.version -DforceStdout=true)" > /etc/profile.d/pom_version.sh'

RUN chmod u+x /etc/profile.d/pom_version.sh

RUN POM_VERSION=`/etc/profile.d/pom_version.sh` && cp /app/pipelines/streaming-atomfeed/target/streaming-atomfeed-bundled-$POM_VERSION.jar /app/app.jar

FROM adoptopenjdk/maven-openjdk8 as run

LABEL org.opencontainers.image.source https://github.com/i-tech-uw/openmrs-fhir-analytics

WORKDIR /app/openmrs-fhir-analytics

COPY --from=build /app/app.jar /app.jar

ENV OPENMRS_URL=""
ENV OPENMRS_USERNAME=""
ENV OPENMRS_PASSWORD=""
ENV SINK_URL=""
ENV SINK_USERNAME=""
ENV SINK_PASSWORD=""

ENTRYPOINT mvn exec:java -pl streaming-atomfeed -Dexec.mainClass=org.openmrs.analytics.FhirStreaming \
    -Dexec.args="--openmrsServerUrl=$OPENMRS_URL --openmrsUserName=$OPENMRS_USERNAME --openmrsPassword=$OPENMRS_PASSWORD --fhirSinkPath=$SINK_URL --sinkUserName=$SINK_USERNAME --sinkPassword=$SINK_PASSWORD"
