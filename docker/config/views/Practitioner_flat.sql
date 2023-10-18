CREATE OR REPLACE VIEW Practitioner_flat AS
SELECT P.id AS prac_id, P.active, PA.city, PA.country, P.gender,
  PQCC.system AS qualification_system, PQCC.code AS qualification_code
FROM Practitioner AS P LATERAL VIEW OUTER explode(P.address) AS PA
  LATERAL VIEW OUTER explode(P.qualification) AS PQ
  LATERAL VIEW OUTER explode(PQ.code.coding) AS PQCC
;