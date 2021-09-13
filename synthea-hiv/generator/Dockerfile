# Dockerfile used to create gobbledegook/synthea

FROM openjdk:16-jdk-alpine
RUN apk --no-cache add openssl wget git curl

RUN wget https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
COPY hiv_simple.json synthea-hiv/hiv_simple.json
COPY hiv_simple synthea-hiv/hiv_simple

ENV POP 100

CMD java -jar synthea-with-dependencies.jar \
    -p ${POP} \
    --exporter.years_of_history 0 \
    --generate.only_alive_patients true \
    -d /synthea-hiv \
    -m hiv_simple \
    -s 123