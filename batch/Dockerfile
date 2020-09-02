FROM openjdk:8-jre-alpine
ENV artifact fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar
WORKDIR /app
COPY ./target/${artifact} /app
EXPOSE 8080
ENTRYPOINT "java -cp ${artifact} org.openmrs.analytics.FhirEtl --serverUrl=${SOURCE_URL} --username=${SOURCE_USERNAME} --password=${SOURCE_PW} --fhirStoreUrl=${SINK_URL}"
