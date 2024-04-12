CREATE OR REPLACE VIEW Encounter_flat AS
SELECT E.id AS id, E.status, ETC.system AS type_sys,
  ETC.code AS type_code, E.subject.PatientId AS patient_id,
  EP.individual.practitionerId AS practitioner_id,
  EL.location.locationId AS location_id,
  E.serviceProvider.organizationId AS service_org_id,
  E.period.start, E.period.end, E.episodeOfCare.EpisodeOfCareId
FROM Encounter AS E LATERAL VIEW OUTER explode(type) AS ET
  LATERAL VIEW OUTER explode(ET.coding) AS ETC
  LATERAL VIEW OUTER explode(E.participant) AS EP
  LATERAL VIEW OUTER explode(E.location) AS EL
;
