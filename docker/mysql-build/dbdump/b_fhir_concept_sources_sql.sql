USE `openmrs`;
LOCK TABLES `fhir_concept_source` WRITE;
/*!40000 ALTER TABLE `fhir_concept_source` DISABLE KEYS */;
INSERT INTO `fhir_concept_source` VALUES (1,41,'http://loinc.org','LOINC',NULL,1,'2021-02-12 05:29:49',NULL,NULL,0,NULL,NULL,NULL,'249b13c8-72fa-4b96-8d3d-b200efed985e'),(2,21,'https://openconceptlab.org/orgs/CIEL/sources/CIEL','CIEL',NULL,1,'2021-02-12 05:35:03',NULL,NULL,0,NULL,NULL,NULL,'2b3c1ff8-768a-102f-83f4-12313b04a615'),(3,40,'http://snomed.info/sct','SNOMED-CT',NULL,1,'2021-02-12 07:22:42',NULL,NULL,0,NULL,NULL,NULL,'70bece6c-6cfa-11eb-9439-0242ac130002'),(4,3,'http://hl7.org/fhir/sid/icd-10 ','ICD-10-WHO',NULL,1,'2021-02-12 07:22:42',NULL,NULL,0,NULL,NULL,NULL,'70bed3b2-6cfa-11eb-9439-0242ac130002'),(5,39,'http://hl7.org/fhir/sid/icd-11','ICD-11-WHO',NULL,1,'2021-02-12 07:22:42',NULL,NULL,0,NULL,NULL,NULL,'70bed48e-6cfa-11eb-9439-0242ac130002'),(6,13,'http://www.ampathkenya.org','AMPATH',NULL,1,'2021-02-12 07:22:42',NULL,NULL,0,NULL,NULL,NULL,'70bed48e-6cfa-11eb-9439-0242ac130045');
/*!40000 ALTER TABLE `fhir_concept_source` ENABLE KEYS */;
UNLOCK TABLES;
