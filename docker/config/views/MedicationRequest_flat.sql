CREATE OR REPLACE VIEW MedicationRequest_flat AS
SELECT M.id AS med_req_id, M.subject.patientId AS patient_id, M.status,
  MSC.system AS statusReason_sys, MSC.code AS statusReason_code,
  MMCC.system, MMCC.code, M.intent, M.doNotPerform,
  M.performer.practitionerId AS practitioner_id
FROM MedicationRequest AS M
  LATERAL VIEW OUTER explode(M.statusReason.coding) AS MSC
  LATERAL VIEW OUTER explode(M.medication.codeableConcept.coding) AS MMCC
;
