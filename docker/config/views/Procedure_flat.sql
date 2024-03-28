CREATE OR REPLACE VIEW Procedure_flat AS
SELECT P.id AS id, P.subject.patientId AS patient_id,
  P.encounter.encounterId AS encounter_id, PCC.system, PCC.code,
  PP.actor.practitionerId AS practitioner_id,
  P.performed.period.start AS period_start,
  P.performed.period.`end` AS period_end,
  P.location.locationId AS location_id, P.status
FROM Procedure AS P LATERAL VIEW OUTER explode(P.code.coding) AS PCC
  LATERAL VIEW OUTER explode(P.performer) AS PP
;