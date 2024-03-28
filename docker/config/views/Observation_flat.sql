CREATE OR REPLACE VIEW Observation_flat AS
SELECT O.id AS id, O.subject.patientId AS patient_id,
  O.encounter.encounterId as encounter_id,
  O.status, OCC.code, OCC.`system` AS code_sys,
  O.value.quantity.value AS val_quantity,
  OVCC.code AS val_code, OVCC.`system` AS val_sys,
  O.effective.dateTime AS obs_date,
  OCatC.`system` AS category_sys,
  OCatC.code AS category_code
FROM Observation AS O LATERAL VIEW OUTER explode(code.coding) AS OCC
  LATERAL VIEW OUTER explode(O.value.codeableConcept.coding) AS OVCC
  LATERAL VIEW OUTER explode(O.category) AS OCat
  LATERAL VIEW OUTER explode(OCat.coding) AS OCatC
;