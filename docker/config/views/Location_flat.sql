CREATE OR REPLACE VIEW Location_flat AS
SELECT L.id AS id, L.status, L.name, L.address.city,
  L.address.country, L.managingOrganization.organizationId AS org_id,
  L.position.longitude, L.position.latitude, L.position.altitude
FROM Location AS L
;
