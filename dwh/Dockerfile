FROM openjdk:8-jdk-slim
### multi-stage builds requires Docker > 17
COPY --from=python:3.7 / /
COPY . /dwh
WORKDIR /dwh
ENV PARQUET_PATH="/dwh/input"
ENV OUTPUT_CSV="indicators_output_XXXXXX.csv"

### Install Dependancies
RUN pip3 install pyspark
RUN pip3 install -r requirements.txt

#### RUN
EXPOSE 4040
ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]