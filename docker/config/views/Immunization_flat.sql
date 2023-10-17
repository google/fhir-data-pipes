CREATE OR REPLACE VIEW Immunization_flat AS
SELECT I.id AS imm_id, I.patient.patientId AS patient_id,
  I.encounter.encounterId AS encounter_id, I.status, IVC.system, IVC.code,
  I.occurrence.DateTime, IP.actor.PractitionerId, IP.actor.OrganizationId
FROM Immunization AS I LATERAL VIEW OUTER explode(I.vaccineCode.coding) AS IVC
  LATERAL VIEW OUTER explode(I.performer) AS IP
;