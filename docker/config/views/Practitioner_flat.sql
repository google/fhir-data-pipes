CREATE OR REPLACE VIEW Practitioner_flat AS
SELECT P.id AS id, P.active, PN.family, PNG AS given, PA.city,
  PA.country, P.gender, PQCC.system AS qualification_system,
  PQCC.code AS qualification_code
FROM Practitioner AS P LATERAL VIEW OUTER explode(name) AS PN
  LATERAL VIEW OUTER explode(PN.given) AS PNG
  LATERAL VIEW OUTER explode(P.address) AS PA
  LATERAL VIEW OUTER explode(P.qualification) AS PQ
  LATERAL VIEW OUTER explode(PQ.code.coding) AS PQCC
;