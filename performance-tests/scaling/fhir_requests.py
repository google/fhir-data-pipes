import os
import time
import requests
import random

prefix = "http://localhost:8080/fhir"

def runRequests():
    patients = runRequest("/Patient?_count=100")
    if patients:
        patient_ids = [entry['resource']['id'] for entry in patients.get('entry', [])]
        random_index = random.randint(0, len(patient_ids) - 1)
        selected_id = patient_ids[random_index]

        # Step 4: Fetch Details
        runRequest(f"/Patient/{selected_id}")
        runRequest(f"/Encounter?patient={selected_id}")
        runRequest(f"/Observation?patient={selected_id}")
        runRequest(f"/MedicationRequest?patient={selected_id}&status=active")


def runRequest(request: str):
    start = time.time()
    response = requests.get(f'{prefix}{request}')
    if response.status_code == 200:
        log(request, start)
        return response.json()
    else:
        log(f"Error {response.status_code} in {request}", start)
        return None


def log(description, start):
    end = time.time()
    full_log = f"{start}\t{description}\t{end - start}"
    print(full_log)
    os.system(f"""echo "{full_log}" >> ~/fhir_requests_log.tsv""")


runRequests()