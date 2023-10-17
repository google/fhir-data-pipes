CREATE OR REPLACE VIEW PractitionerRole_flat AS
SELECT P.id AS pr_id, P.practitioner.practitionerId as practitioner_id,
  P.active, P.organization.organizationId AS organization_id,
  PCC.system, PCC.code
FROM PractitionerRole AS P LATERAL VIEW OUTER explode(P.code) AS PC
  LATERAL VIEW OUTER explode(PC.coding) AS PCC
;