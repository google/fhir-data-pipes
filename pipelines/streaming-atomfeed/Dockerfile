FROM maven:3.6.3-openjdk-8

WORKDIR /app/openmrs-fhir-analytics

COPY ./pom.xml /app/openmrs-fhir-analytics/pom.xml
COPY ./streaming-atomfeed/pom.xml /app/openmrs-fhir-analytics/streaming-atomfeed/pom.xml
COPY ./streaming-binlog/pom.xml /app/openmrs-fhir-analytics/streaming-binlog/pom.xml
COPY ./common/pom.xml /app/openmrs-fhir-analytics/common/pom.xml
COPY ./batch/pom.xml /app/openmrs-fhir-analytics/batch/pom.xml

RUN mvn -B -pl streaming-atomfeed -am verify --fail-never

COPY ./common /app/openmrs-fhir-analytics/common
COPY ./streaming-atomfeed /app/openmrs-fhir-analytics/streaming-atomfeed
COPY ./batch /app/openmrs-fhir-analytics/batch
COPY ./streaming-binlog /app/openmrs-fhir-analytics/streaming-binlog

RUN mvn -B install -DskipTests -Dlicense.skip=true -pl streaming-atomfeed -am

ENV OPENMRS_URL=""
ENV OPENMRS_USERNAME=""
ENV OPENMRS_PASSWORD=""
ENV SINK_URL=""
ENV SINK_USERNAME=""
ENV SINK_PASSWORD=""

ENTRYPOINT mvn exec:java -pl streaming-atomfeed -Dexec.mainClass=org.openmrs.analytics.FhirStreaming \
    -Dexec.args="--openmrsServerUrl=$OPENMRS_URL --openmrsUserName=$OPENMRS_USERNAME --openmrsPassword=$OPENMRS_PASSWORD --fhirSinkPath=$SINK_URL --sinkUserName=$SINK_USERNAME --sinkPassword=$SINK_PASSWORD"
