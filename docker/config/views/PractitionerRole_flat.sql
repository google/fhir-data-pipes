CREATE OR REPLACE VIEW PractitionerRole_flat AS
SELECT P.id AS id, P.practitioner.practitionerId as practitioner_id,
  P.active, P.organization.organizationId AS organization_id,
  PCC.system, PCC.code,
  PSC.system AS specialty_sys, PSC.code AS specialty_code,
  PL.LocationId, PH.HealthcareServiceId
FROM PractitionerRole AS P
  LATERAL VIEW OUTER explode(P.code) AS PC
  LATERAL VIEW OUTER explode(PC.coding) AS PCC
  LATERAL VIEW OUTER explode(P.specialty) AS PS
  LATERAL VIEW OUTER explode(PS.coding) AS PSC
  LATERAL VIEW OUTER explode(P.location) AS PL
  LATERAL VIEW OUTER explode(P.healthcareService) AS PH
;
