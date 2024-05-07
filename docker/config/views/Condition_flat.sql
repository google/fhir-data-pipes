CREATE OR REPLACE VIEW Condition_flat AS
SELECT C.id AS id, C.subject.patientId AS patient_id,
  C.encounter.encounterId AS encounter_id, CCC.system, CCC.code,
  CCatC.code AS category, CClC.code AS clinical_status,
  CVC.code AS verification_status, C.onset.DateTime AS onset_datetime
FROM Condition AS C LATERAL VIEW OUTER explode(C.code.coding) AS CCC
  LATERAL VIEW OUTER explode(C.category) AS CCat
  LATERAL VIEW OUTER explode(CCat.coding) AS CCatC
  LATERAL VIEW OUTER explode(C.clinicalStatus.coding) AS CClC
  LATERAL VIEW OUTER explode(C.verificationStatus.coding) AS CVC
;