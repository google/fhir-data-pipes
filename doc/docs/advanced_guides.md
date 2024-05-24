# Advanced guides

## Transformation to parquet using SQL-on-FHIR schema

*   Document the SQL-on-FHIR schema:
    *   Recursive depth
    *   Contained resources
    *   Explain the FHIR to AvroConverter and Bunsen
    *   FHIR version support (and future plans)
    *   Support for extensions
    *   How to use StructureDefinitions → link to tutorial

## Parquet representation
*   Note about this
*   Horizontal scalability
*   Managing parquet files

## Pipelines Controller
https://github.com/google/fhir-data-pipes/wiki/Try-out-the-FHIR-Pipelines-Controller

## Performance
Highlight the performance numbers especially for scaling and across single versus multiple machines

## Apache Beam runners

Apache Beam supports a range of different runners depending on the deployment architecture. 

*   Describe the different beam runners
*   How to configure via the pipeline
*   Important config options e.g. setting memory etc

## Deployment patterns**

| Approach | Scenarios | Considerations |
| -------- | ----------| -------------- |
| RDBMS using "lossy" schema (defined as ViewDefinition Resources) | Using a relational database to power dashboards or reporting | By design this will provide a constrained set of variables in the views |
| Distributed "lossless" parquet based DWH and distributed query engine | Need for a horizontally scalable architecture | Will need to manage both distributed storage (Parquet) and a distributed query engine |
| Non-distributed "lossless" parquet based DWH | Want to leverage parquet with a non-distributed OLAP database engine (such as duckdb) | xxx |

## Web Control Panel
The web control panel is a basic spring application provided to make interacting with the pipeline controller easier. It is not designed to be a full production ready “web admin” panel.

It has the following features:

*   Initiate full and incremental pipeline runs
*   Monitor errors when running pipelines
*   Recreate view tables
*   View configuration settings
*   Access sample jupyter notebooks and ViewDefinition editor

## Querying exported Parquet files

Assumes that you have completed ....

### Handle nested fields

One major challenge when querying exported data is that FHIR resources have many nested fields. One approach is to use `LATERAL VIEW` with `EXPLODE` to flatten repeated fields and then filter for specific values of interest.


#### Example queries

The following queries explore the [sample data](https://github.com/google/fhir-data-pipes/tree/master/synthea-hiv/sample_data) loaded when using a [local test server](https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers). They leverage `LATERAL VIEW` with `EXPLODE` to flatten the Observation.code.coding repeated field and filter for specific observation codes.

Note that the synthetic sample data simulates HIV patients. Observations for HIV viral load use the following code, which is not the actual LOINC code:


```json
[
   {
      "id":null,
      "coding":[
         {
            "id":null,
            "system":"http://loinc.org",
            "version":null,
            "code":"856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "display":"HIV viral load",
            "userSelected":null
         }
      ],
      "text":"HIV viral load"
   }
]
```

##### Flattening a resource table based on a field

Let's say we are interested only in certain observation codes. For working with repeated fields
like `Observation.code.coding` sometime it is easier to first "flatten" the table on that field.
Conceptually this means that an Observation row with say 4 codes will be repeated 4 times
where each row has exactly one of those 4 values. Here is a query that does that selecting only
rows with "viral load" observations (code 856): 

```hiveql
SELECT O.id AS obs_id, OCC.`system`, OCC.code, O.status AS status, O.value.quantity.value AS value
FROM Observation AS O LATERAL VIEW explode(code.coding) AS OCC 
WHERE OCC.`system` = 'http://loinc.org'
  AND OCC.code LIKE '856A%'
LIMIT 4;
```
Sample output:
```
+---------+-------------------+---------------------------------------+---------+-----------+
| obs_id  |      system       |                 code                  | status  |   value   |
+---------+-------------------+---------------------------------------+---------+-----------+
| 10393   | http://loinc.org  | 856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 224650.0  |
| 12446   | http://loinc.org  | 856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 823010.0  |
| 14456   | http://loinc.org  | 856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 166100.0  |
| 15991   | http://loinc.org  | 856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 64630.0   |
+---------+-------------------+---------------------------------------+---------+-----------+

```

##### Find patients with an observed viral load higher than a threshold
Now, let's say we are interested only in cases with high viral load; and for each patient
we need some demographic information too. We can use the flat table we created above and
join it with the Patient resource table:


```hiveql
SELECT P.id AS pid, P.name.family AS family, P.gender AS gender, O.id AS obs_id, OCC.`system`,
  OCC.code, O.status AS status, O.value.quantity.value AS value
FROM Patient AS P, Observation AS O LATERAL VIEW explode(code.coding) AS OCC 
WHERE P.id = O.subject.PatientId
  AND OCC.`system` = 'http://loinc.org'
  AND OCC.code LIKE '856A%'
  AND O.value.quantity.value > 10000
LIMIT 4;
```

Sample output:

```
+--------+-----------------------------------+---------+---------+-------------------+---------------------------------------+---------+-----------+
|  pid   |              family               | gender  | obs_id  |      system       |                 code                  | status  |   value   |
+--------+-----------------------------------+---------+---------+-------------------+---------------------------------------+---------+-----------+
| 10091  | ["Fritsch593"]                    | male    | 10393   | http://loinc.org  | 856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 224650.0  |
| 11689  | ["Dickinson688"]                  | male    | 12446   | http://loinc.org  | 856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 823010.0  |
| 13230  | ["Jerde200","Ruecker817"]         | female  | 14456   | http://loinc.org  | 856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 166100.0  |
| 15315  | ["Pfeffer420","Pfannerstill264"]  | female  | 15991   | http://loinc.org  | 856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 64630.0   |
+--------+-----------------------------------+---------+---------+-------------------+---------------------------------------+---------+-----------+
```

##### Count all viral-load observations

```hiveql
SELECT COUNT(0)
FROM (
  SELECT P.id AS pid, P.name.family AS family, P.gender AS gender, O.id AS obs_id, OCC.`system`,
    OCC.code, O.status AS status, O.value.quantity.value
  FROM Patient AS P, Observation AS O LATERAL VIEW explode(code.coding) AS OCC 
  WHERE P.id = O.subject.PatientId
    AND OCC.`system` = 'http://loinc.org'
    AND OCC.code LIKE '856A%'
);
```

Sample output:

```
+-----------+
| count(0)  |
+-----------+
| 265       |
+-----------+

```

### Use Views to reduce complexity

Once you have a query that filters to the data you're interested in, create a view with a simpler schema to work with in the future. This is a good way to create building blocks to combine with other data and create more complex queries.


##### Observations of patients starting an Anti-retroviral plan in 2010

```hiveql
SELECT
  O.id AS obs_id, OCC.`system`, OCC.code, O.status AS status,
  OVCC.code AS value_code, O.subject.PatientId AS patient_id
FROM Observation AS O LATERAL VIEW explode(code.coding) AS OCC
  LATERAL VIEW explode(O.value.codeableConcept.coding) AS OVCC
WHERE OCC.code LIKE '1255%'
  AND OVCC.code LIKE "1256%"
  AND YEAR(O.effective.dateTime) = 2010
LIMIT 1;
```


Sample output:

```
+---------+-------------------+---------------------------------------+---------+---------------------------------------+-------------+
| obs_id  |      system       |                 code                  | status  |              value_code               | patient_id  |
+---------+-------------------+---------------------------------------+---------+---------------------------------------+-------------+
| 33553   | http://loinc.org  | 1255AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | final   | 1256AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  | 32003       |
+---------+-------------------+---------------------------------------+---------+---------------------------------------+-------------+
```

##### Create a corresponding view

```hiveql
CREATE VIEW obs_arv_plan AS
SELECT
  O.id AS obs_id, OCC.`system`, OCC.code, O.status AS status,
  OVCC.code AS value_code, O.subject.PatientId AS patient_id
FROM Observation AS O LATERAL VIEW explode(code.coding) AS OCC
  LATERAL VIEW explode(O.value.codeableConcept.coding) AS OVCC
WHERE OCC.code LIKE '1255%'
  AND OVCC.code LIKE "1256%"
  AND YEAR(O.effective.dateTime) = 2010;
```


##### Count cases of Anti-retroviral plans started in 2010

```hiveql
SELECT COUNT(0) FROM obs_arv_plan ;
```


Sample output:

```
+-----------+
| count(0)  |
+-----------+
| 2         |
+-----------+

```



##### Compare Patient data with view based on observations

```hiveql
SELECT P.id AS pid, P.name.family AS family, P.gender AS gender, COUNT(0) AS num_start
FROM Patient P, obs_arv_plan
WHERE P.id = obs_arv_plan.patient_id
GROUP BY P.id, P.name.family, P.gender
ORDER BY num_start DESC
LIMIT 10;
```


Sample output:

```
+--------+-------------------+---------+------------+
|  pid   |      family       | gender  | num_start  |
+--------+-------------------+---------+------------+
| 20375  | ["VonRueden376"]  | male    | 1          |
| 32003  | ["Terry864"]      | male    | 1          |
+--------+-------------------+---------+------------+

```
