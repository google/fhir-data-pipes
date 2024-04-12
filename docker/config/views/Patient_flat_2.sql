CREATE OR REPLACE VIEW Patient_flat_2 AS
SELECT P.id AS id, P.active, PN.family, PNG AS given, P.gender,
  P.deceased.Boolean AS deceased, P.birthDate,
  YEAR(current_date()) - YEAR(P.birthDate) AS age,
  PA.country, PG.practitionerId AS practitioner_id,
  P.managingOrganization.organizationId AS organization_id
FROM Patient AS P LATERAL VIEW OUTER explode(name) AS PN
  LATERAL VIEW OUTER explode(PN.given) AS PNG
  LATERAL VIEW OUTER explode(P.address) AS PA
  LATERAL VIEW OUTER explode(P.generalPractitioner) AS PG
;