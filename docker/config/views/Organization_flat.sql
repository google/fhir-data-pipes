CREATE OR REPLACE VIEW Organization_flat AS
SELECT O.id AS id, O.active, O.name, OA.city, OA.country,
  OTC.system, OTC.code
FROM Organization AS O LATERAL VIEW OUTER explode(O.address) AS OA
   LATERAL VIEW OUTER explode(O.type) AS OT
   LATERAL VIEW OUTER explode(OT.coding) AS OTC
;