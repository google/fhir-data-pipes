CREATE OR REPLACE VIEW MedicationRequest_flat AS
SELECT M.id AS id, M.subject.patientId AS patient_id,
  M.encounter.encounterId AS encounter_id, M.status,
  MSC.system AS statusReason_sys, MSC.code AS statusReason_code,
  MMCC.system AS med_system, MMCC.code AS med_code,
  M.medication.reference.medicationId AS med_id,
  M.intent, M.doNotPerform,
  M.requester.practitionerId AS req_practitioner_id,
  M.performer.practitionerId AS perf_practitioner_id
FROM MedicationRequest AS M
  LATERAL VIEW OUTER explode(M.statusReason.coding) AS MSC
  LATERAL VIEW OUTER explode(M.medication.codeableConcept.coding) AS MMCC
;
