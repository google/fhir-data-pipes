CREATE OR REPLACE VIEW DiagnosticReport_flat AS
SELECT D.id AS id, D.subject.patientId AS patient_id,
  D.encounter.EncounterId AS encounter_id,
  DCC.system, DCC.code, DR.observationId AS result_obs_id,
  D.status, DP.PractitionerId AS practitioner_id,
  DCatC.system AS category_sys, DCatC.code AS category_code,
  DConC.system AS conclusion_sys, DConC.code AS conclusion_code,
  D.conclusion
FROM DiagnosticReport AS D LATERAL VIEW OUTER explode(D.result) AS DR
  LATERAL VIEW OUTER explode(D.code.coding) AS DCC
  LATERAL VIEW OUTER explode(D.performer) AS DP
  LATERAL VIEW OUTER explode(D.category) AS DCat
  LATERAL VIEW OUTER explode(DCat.coding) AS DCatC
  LATERAL VIEW OUTER explode(D.conclusionCode) AS DCon
  LATERAL VIEW OUTER explode(DCon.coding) AS DConC
;
