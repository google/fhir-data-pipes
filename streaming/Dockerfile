FROM maven:3.6.3-openjdk-8 as build
WORKDIR /app
CMD ["mvn", "clean", "install", "-DskipTests"]


FROM maven:3.6.3-openjdk-8 as run
WORKDIR /app
# ENTRYPOINT ["mvn", "exec:java", "-pl", "streaming", "-Dexec.mainClass=org.openmrs.analytics.FhirStreaming"]
ENTRYPOINT mvn exec:java -pl streaming -Dexec.mainClass=org.openmrs.analytics.FhirStreaming