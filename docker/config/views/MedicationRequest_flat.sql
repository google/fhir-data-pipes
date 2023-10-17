CREATE OR REPLACE VIEW MedicationRequest_flat AS
SELECT M.id AS med_req_id, M.subject.patientId AS patient_id, M.status,
  MMCC.system, MMCC.code, M.performer.practitionerId AS practitioner_id
FROM MedicationRequest AS M
  LATERAL VIEW OUTER explode(M.medication.codeableConcept.coding) AS MMCC
;