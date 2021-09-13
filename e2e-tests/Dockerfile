FROM adoptopenjdk/maven-openjdk8

RUN apt-get update && apt-get install -y jq  python3.7 python3-pip 
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1
RUN pip3 install virtualenv

COPY batch-mode-tests.sh batch-mode-tests.sh
COPY parquet-tools-1.11.1.jar  parquet-tools-1.11.1.jar
ENV TEST_TYPE="NON_JDBC"
ENV DOCKER_NETWORK="--use_docker_network"
ENV HOME_DIR="/workspace/e2e-tests"

ENTRYPOINT cd ${HOME_DIR}; ./batch-mode-tests.sh ${HOME_DIR} ${TEST_TYPE} ${DOCKER_NETWORK}