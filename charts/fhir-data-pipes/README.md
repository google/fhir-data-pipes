
# Fhir-data-pipes

An open-source pipeline to transform data from a FHIR server (like HAPI, GCP FHIR store, or even OpenMRS) using the FHIR format into a data warehouse based on Apache Parquet files, or another FHIR server.

## TL;DR

```bash
$ helm repo add opensrp-fhir-data-pipes https://fhir-data-pipes.helm.smartregister.org
$ helm install fhir-data-pipes opensp-fhir-data-pipes/fhir-data-pipes
```

## Introduction

This chart bootstraps  [fhir-data-pipes](https://github.com/onaio/fhir-data-pipes) deployment on a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Kubernetes 1.12+
- Helm 3.1.0

## Configuration

The following table lists the configurable parameters of the Fhir-data-pipes chart and their default values.

| Parameter                                             | Description | Default                                                                                               |
|-------------------------------------------------------|-------------|-------------------------------------------------------------------------------------------------------|
| `replicaCount`                                        |             | `1`                                                                                                   |
| `image.repository`                                    |             | `"onaio/fhir-data-pipes"`                                                                             |
| `image.pullPolicy`                                    |             | `"IfNotPresent"`                                                                                      |
| `image.tag`                                           |             | `"master"`                                                                                            |
| `imagePullSecrets`                                    |             | `[]`                                                                                                  |
| `nameOverride`                                        |             | `""`                                                                                                  |
| `fullnameOverride`                                    |             | `""`                                                                                                  |
| `serviceAccount.create`                               |             | `true`                                                                                                |
| `serviceAccount.annotations`                          |             | `{}`                                                                                                  |
| `serviceAccount.name`                                 |             | `""`                                                                                                  |
| `podAnnotations`                                      |             | `{}`                                                                                                  |
| `podSecurityContext`                                  |             | `{}`                                                                                                  |
| `securityContext`                                     |             | `{}`                                                                                                  |
| `service.type`                                        |             | `"ClusterIP"`                                                                                         |
| `service.port`                                        |             | `8080`                                                                                                |
| `ingress.enabled`                                     |             | `false`                                                                                               |
| `ingress.className`                                   |             | `""`                                                                                                  |
| `ingress.annotations`                                 |             | `{}`                                                                                                  |
| `ingress.hosts`                                       |             | `[{"host": "fhir-data-pipes.local", "paths": [{"path": "/", "pathType": "ImplementationSpecific"}]}]` |
| `ingress.tls`                                         |             | `[]`                                                                                                  |
| `resources`                                           |             | `null`                                                                                                |
| `autoscaling.enabled`                                 |             | `false`                                                                                               |
| `autoscaling.minReplicas`                             |             | `1`                                                                                                   |
| `autoscaling.maxReplicas`                             |             | `100`                                                                                                 |
| `autoscaling.targetCPUUtilizationPercentage`          |             | `80`                                                                                                  |
| `nodeSelector`                                        |             | `{}`                                                                                                  |
| `tolerations`                                         |             | `[]`                                                                                                  |
| `affinity`                                            |             | `{}`                                                                                                  |
| `recreatePodsWhenConfigMapChange`                     |             | `true`                                                                                                |
| `livenessProbe.httpGet.path`                          |             | `"/"`                                                                                                 |
| `livenessProbe.httpGet.port`                          |             | `"http"`                                                                                              |
| `readinessProbe.httpGet.path`                         |             | `"/"`                                                                                                 |
| `readinessProbe.httpGet.port`                         |             | `"http"`                                                                                              |
| `hapi.postgres.databaseService`                       |             | `"postgresql"`                                                                                        |
| `hapi.postgres.databaseHostName`                      |             | `""`                                                                                                  |
| `hapi.postgres.databasePort`                          |             | `"5432"`                                                                                              |
| `hapi.postgres.databaseUser`                          |             | `""`                                                                                                  |
| `hapi.postgres.databasePassword`                      |             | `""`                                                                                                  |
| `hapi.postgres.databaseName`                          |             | `""`                                                                                                  |
| `thriftserver.hive.databaseService`                   |             | `"hive2"`                                                                                             |
| `thriftserver.hive.databaseHostName`                  |             | `"spark-thriftserver"`                                                                                |
| `thriftserver.hive.databasePort`                      |             | `"10000"`                                                                                             |
| `thriftserver.hive.databaseUser`                      |             | `"hive"`                                                                                              |
| `thriftserver.hive.databasePassword`                  |             | `""`                                                                                                  |
| `thriftserver.hive.databaseName`                      |             | `"default"`                                                                                           |
| `applicationConfig.fhirdata.fhirServerUrl`            |             | `""`                                                                                                  |
| `applicationConfig.fhirdata.dbConfig`                 |             | `"/app/config/hapi-postgres-config.json"`                                                             |
| `applicationConfig.fhirdata.dwhRootPrefix`            |             | `"/dwh/controller_DWH"`                                                                               |
| `applicationConfig.fhirdata.thriftserverHiveConfig`   |             | `"/app/config/thrifter-hive-config.json"`                                                             |
| `applicationConfig.fhirdata.incrementalSchedule`      |             | `"* * * * * *"`                                                                                       |
| `applicationConfig.fhirdata.resourceList`             |             | `"PatientEncounterObservation"`                                                                       |
| `applicationConfig.fhirdata.maxWorkers`               |             | `"10"`                                                                                                |
| `applicationConfig.fhirdata.createHiveResourceTables` |             | `"true"`                                                                                              |
| `applicationConfig.fhirdata.hiveJdbcDriver`           |             | `"org.apache.hive.jdbc.HiveDriver"`                                                                   |
| `initContainers`                                      |             | `null`                                                                                                |
| `extraVolumes`                                        |             | `null`                                                                                                |
| `extraVolumeMounts`                                   |             | `null`                                                                                                |
| `extraConfigMaps`                                     |             | `null`                                                                                                |
| `env`                                                 |             | `null`                                                                                                |
| `pdb.enabled`                                         |             | `false`                                                                                               |
| `pdb.minAvailable`                                    |             | `""`                                                                                                  |
| `pdb.maxUnavailable`                                  |             | `1`                                                                                                   |
| `vpa.enabled`                                         |             | `false`                                                                                               |
| `vpa.updatePolicy.updateMode`                         |             | `"Off"`                                                                                               |
| `vpa.resourcePolicy`                                  |             | `{}`                                                                                                  |
