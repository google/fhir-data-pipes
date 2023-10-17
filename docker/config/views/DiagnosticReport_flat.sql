CREATE OR REPLACE VIEW DiagnosticReport_flat AS
SELECT D.id AS dr_id, D.subject.patientId AS patient_id,
  D.encounter.EncounterId AS encounter_id,
  DCC.system, DCC.code, DR.observationId AS result_obs_id,
  D.status, DP.PractitionerId AS practitioner_id
FROM DiagnosticReport AS D LATERAL VIEW OUTER explode(D.result) AS DR
  LATERAL VIEW OUTER explode(D.code.coding) AS DCC
  LATERAL VIEW OUTER explode(D.performer) AS DP
;