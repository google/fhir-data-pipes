// docker run --net=host --rm -i grafana/k6 run - <fhir_requests.js

import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 1000,
  duration: '60s',
};

export default function () {
  const prefix = "http://localhost:8080/fhir"
  const res = http.get(`${prefix}/Patient?_count=100`);
  check(res, { 'status was 200': (r) => r.status == 200 });
  const patientIds = res.json().entry.map(entry => entry.resource.id);
  // Step 3: Select Random Patient
  const randomIndex = Math.floor(Math.random() * patientIds.length);
  const selectedId = patientIds[randomIndex];
  http.get(prefix + "/Patient/" + selectedId)
    http.get(prefix + "/Encounter?patient=" + selectedId)
    http.get(prefix + "/Observation?patient=" + selectedId)
    http.get(prefix + "/MedicationRequest?patient=" + selectedId + "&status=active")

  sleep(1);
}