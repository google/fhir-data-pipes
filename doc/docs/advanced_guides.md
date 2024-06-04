# Advanced guides

* Pre-defined views
* Configuration settings
* Querying SQL-on-FHIR Data

## Pre-defined views
A set of SQL queries (_&lt;Resource_Name&gt;_flat.**sql**_) and ViewDefinition (_&lt;Resource_Name&gt;_flat.**json**_) resources has been provided for defining flat views for common FHIR resources.

The currently supported list (as at 1st June, 2024), which can be found in the `docker/config/views' directory are:

1. Condition
* DiagnosticReport
* Encounter
* Immunization
* Location
* Medicationrequest
* Observation
* Organization
* Patient
* Practitioner
* PractitionerRole
* Procedure

### SQL virtual views
These are samples of more complex SQL-on-FHIR queries for defining flat views for common FHIR Resources. These virtual views are applied outside of the pipelines in the downstream SQL query engine. 

The queries, which have .sql suffix can be found here: /docker/config/views (e.g Patient_flat.sql)

An example of a flat view for the Observation Resource is below

```sql
CREATE OR REPLACE VIEW flat_observation AS
SELECT O.id AS obs_id, O.subject.PatientId AS patient_id,
        OCC.`system` AS code_sys, OCC.code,
        O.value.quantity.value AS val_quantity,
        OVCC.code AS val_code, OVCC.`system` AS val_sys,
        O.effective.dateTime AS obs_date
      FROM Observation AS O LATERAL VIEW OUTER explode(code.coding) AS OCC
        LATERAL VIEW OUTER explode(O.value.codeableConcept.coding) AS OVCC
```

Learn more:

[Examples of SQL-on-FHIR-v1 queries for defining views](https://github.com/google/fhir-data-pipes/blob/27d691e91d0fe6ef4c9624acba4e68bca145c973/query/queries_and_views.ipynb) 

### ViewDefinition resource
The [SQL-on-FHIR-v2 specification](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/) defines a standards based pattern for defining Views as FHIRPath expressions in a logical structure to specify the column names and values (as unnested items).

A system (pipeline or library) that implements the “View Layer” of the specification provides a View Runner that is able to process these FHIR ViewDefinition Resources over the “Data Layer” (lossless representation of the FHIR data). The output of this are a set of portable, tabular views that can be consumed by the “Analytics Layer” which is any number of tools that can be used to work with the resulting tabular data.
 
FHIR Data Pipes is a reference implementation of the SQL-on-FHIR-v2 specification:

*   The "View Runner" is by default part of the ETL Pipelines and uses the transformed Parquet files as the “Data Layer”. _This can be extracted to be a stand-alone component if required_

*   When enabled as part of the Pipeline configuration, it will apply the ViewDefinition Resources from the /config/views folder and materialize the resulting tables to the PostgresSQL database.

*   A set of pre-defined ViewDefinitions for common FHIR Resources is provided as part of the default package. These can be adapted, replaced and extended

*   The FHIR Data Pipes provides a simple ViewDefinition Editor which can be used to explore FHIR ViewDefinitions and apply these to individual FHIR Resources

## Querying SQL-on-FHIR Data

One major challenge when querying exported data is that FHIR resources have many nested fields. One approach is to use `LATERAL VIEW` with `EXPLODE` to flatten repeated fields and then filter for specific values of interest.

### Example queries

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

### Flattening a resource table based on a field

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

#### Find patients with an observed viral load higher than a threshold
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

#### Count all viral-load observations

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
