FROM alpine/git as clone
WORKDIR /app

# TODO: Look over fixes and create PRs as needed to remove this requirement
RUN ["git", "clone", "-b", "db-url-port-fix", "https://github.com/pmanko/atomfeed.git"]
RUN ["git", "clone", "-b", "isanteplus-fixes", "https://github.com/pmanko/openmrs-module-atomfeed.git"]

FROM maven:3.6.3-openjdk-8 as build
COPY --from=clone /app/atomfeed /app/atomfeed
COPY --from=clone /app/openmrs-module-atomfeed /app/openmrs-module-atomfeed

WORKDIR /app/atomfeed
RUN mvn -B install -DskipTests

WORKDIR /app/openmrs-module-atomfeed
RUN mvn -B install -DskipTests

WORKDIR /app/openmrs-fhir-analytics
COPY ./pom.xml /app/openmrs-fhir-analytics/pom.xml
COPY ./streaming-atomfeed/pom.xml /app/openmrs-fhir-analytics/streaming-atomfeed/pom.xml
COPY ./streaming-binlog/pom.xml /app/openmrs-fhir-analytics/streaming-binlog/pom.xml
COPY ./common/pom.xml /app/openmrs-fhir-analytics/common/pom.xml
COPY ./batch/pom.xml /app/openmrs-fhir-analytics/batch/pom.xml

RUN mvn -P atomfeed-fix -B -pl streaming-atomfeed -am verify --fail-never

COPY . /app/openmrs-fhir-analytics

RUN mvn -B install -P atomfeed-fix -DskipTests -pl streaming-atomfeed -am