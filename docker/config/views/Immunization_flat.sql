CREATE OR REPLACE VIEW Immunization_flat AS
SELECT I.id AS imm_id, I.patient.patientId AS patient_id,
  I.encounter.encounterId AS encounter_id, I.status,
  ISC.system AS statusReason_sys, ISC.code AS statusReason_code,
  IVC.system AS vaccine_sys, IVC.code AS vaccine_code,
  I.occurrence.DateTime, I.location.LocationId AS location_id,
  IP.actor.PractitionerId, IP.actor.OrganizationId
FROM Immunization AS I
  LATERAL VIEW OUTER explode(I.statusReason.coding) AS ISC
  LATERAL VIEW OUTER explode(I.vaccineCode.coding) AS IVC
  LATERAL VIEW OUTER explode(I.performer) AS IP
;
