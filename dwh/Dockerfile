FROM openjdk:8-jre-slim
### multi-stage builds requires Docker > 17
COPY --from=python:3.7-slim / /
ADD . /dwh
WORKDIR /dwh
ENV PARQUET_PATH="/dwh/input"
ENV OUTPUT_CSV="indicators_output"
ENV NUM_DAYS=28
ENV LAST_DATE="2020-12-30"

### Install Dependancies
RUN python -m pip install -r requirements.txt

#### RUN
EXPOSE 4040
ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]