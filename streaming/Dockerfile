FROM alpine/git as clone
WORKDIR /app
RUN git clone https://github.com/pmanko/openmrs-fhir-analytics.git
WORKDIR /app/openmrs-fhir-analytics
RUN git checkout hapi-client

FROM maven:3.6.3-openjdk-8 as build
WORKDIR /app
COPY --from=clone /app/openmrs-fhir-analytics /app
COPY ./src/main/resources/hibernate.default.properties /app/streaming/main/resources/
RUN mvn clean install
ENTRYPOINT mvn exec:java -pl streaming -Dexec.mainClass=org.openmrs.analytics.FhirStreaming




#FROM java:8
#
#ARG USER_HOME_DIR="/root"
#ARG SHA=c35a1803a6e70a126e80b2b3ae33eed961f83ed74d18fcd16909b2d44d7dada3203f1ffe726c17ef8dcca2dcaa9fca676987befeadc9b9f759967a8cb77181c0
#
#RUN mkdir ${USER_HOME_DIR}/target
#
#CMD ["echo", "Adding Files"]
#
#ADD ../target/fhir-warehouse-1.0-SNAPSHOT.jar ${USER_HOME_DIR}/target
#ADD ../target/libs ${USER_HOME_DIR}/target/libs
#
#WORKDIR "${USER_HOME_DIR}/target"
#
#ENTRYPOINT [ "java", "-jar", "fhir-warehouse-1.0-SNAPSHOT.jar"]


#
#FROM openjdk:8-jre-alpine
#ARG artifactid
#ARG version
#ENV artifact ${artifactid}-${version}.jar
#WORKDIR /app
#COPY --from=build /app/target/${artifact} /app
#EXPOSE 8080
#ENTRYPOINT ["sh", "-c"]
#CMD ["java -jar ${artifact}"]
