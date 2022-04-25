# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
r"""Uploads FHIR Bundle transactions to a FHIR endpoint.

This module reads JSON files containing Bundle transactions generated by
Synthea and uploads them to a FHIR server you specify.

Example usage:
    python3 main.py GCP \
    https://healthcare.googleapis.com/v1beta1/projects/terrafhir/locations/us-central1/datasets/example-dataset/fhirStores/hundred/fhir \
    --input_dir  ../sample_data

  python3 main.py OpenMRS http://localhost:8099/openmrs/ws/fhir2/R4 \
    --input_dir  sample_data/fhir --convert_to_openmrs

  python3 main.py HAPI http://localhost:8098/fhir \
    --input_dir  sample_data/fhir
"""

import argparse
import itertools
import json
import logging
import multiprocessing
import pathlib
from typing import Dict, Set

import bundle
import fhir_client
import uploader

_CLIENT_MAP = {
    'GCP': 'GcpClient',
    'OpenMRS': 'OpenMrsClient',
    'HAPI': 'HapiClient'
}

parser = argparse.ArgumentParser(
    description='Upload FHIR Bundles.',
    formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument(
    'sink_type',
    help='FHIR Server which data will be sent to',
    choices=_CLIENT_MAP.keys())

parser.add_argument(
    'fhir_endpoint',
    help=(
        'endpoint to upload to.\n\nFor GCP FHIR Store, the format is '
        'https://healthcare.googleapis.com/v1beta1/projects/PROJECT_ID/'
        'locations/LOCATION/datasets/DATASET/fhirStores/FHIR_STORE/fhir'
        '\n\nFor a local OpenMRS endpoint, it is http://localhost:8099/openmrs/'
        'ws/fhir2/R4\n\nFor a local HAPI endpoint, it is http://localhost:8098/fhir'
    ))

parser.add_argument(
    '--input_dir',
    type=str,
    default='../output/fhir',
    help='directory where JSON files are stored')

parser.add_argument(
    '--convert_to_openmrs',
    action='store_true',
    help=('specify if uploading to OpenMRS. Splits bundle to resources before '
          'uploading'))

parser.add_argument(
    '--cores',
    type=int,
    default=multiprocessing.cpu_count(),
    help='specify number of cores to use . Default is CPU count on machine')


def list_all_files(directory: str) -> Dict[str, Set[pathlib.PosixPath]]:
  """Lists JSON files under a directory.

  Args:
    directory: Directory containing JSON files

  Returns:
   Dictionary listing JSON files for 'hospitals', 'practitioners', and
   'patient_history'
  """
  p = pathlib.Path(directory)
  hospital_file = set(p.glob('hospitalInformation*.json'))
  practitioner_file = set(p.glob('practitionerInformation*.json'))
  patient_history_files = set(
      p.glob('*.json')) - practitioner_file - hospital_file

  file_type_dict = {
      'hospital': hospital_file,
      'practitioner': practitioner_file,
      'patient_history': patient_history_files
  }

  logging.info('%s patients found in %s', len(patient_history_files),
               p.resolve())
  return file_type_dict


def convert_to_bundle(json_file: pathlib.PosixPath) -> bundle.Bundle:
  """Loads content of file to create Bundle object.

  Args:
   json_file: Set containing JSON files that need to be read

  Returns:
    Bundle object
  """
  with open(json_file) as f:
    data = json.loads(f.read())
    return bundle.Bundle(json_file, data)

def create_sink(sink_type: str, url: str) -> uploader.Uploader:
  client_ = getattr(fhir_client, _CLIENT_MAP[sink_type])
  return uploader.Uploader(client_(url))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  args = parser.parse_args()
  json_file_dict = list_all_files(args.input_dir)
  upload_handler = create_sink(args.sink_type, args.fhir_endpoint)

  logging.info("Using %s cores to uplaod", args.cores)
  if args.convert_to_openmrs:
    with multiprocessing.Pool(processes=args.cores) as pool:
      locations_in_store = upload_handler.fetch_location()
      logging.info('Loading patient_history JSON files into memory')
      bundle_list = pool.map(convert_to_bundle,
                             json_file_dict['patient_history'])
      pool.starmap(
          upload_handler.upload_openmrs_bundle,
          zip(bundle_list,itertools.repeat(locations_in_store)))

  else:
    # To post the files to GCP FHIR Store, they require a certain order because
    # Synthea only creates bundles of type transaction and POST. The order is:
    # hospital, practitioner, patient history
    for file_type in ['hospital', 'practitioner', 'patient_history']:
      with multiprocessing.Pool(processes=args.cores) as pool:
        logging.info('Loading %s JSON files into memory', file_type)
        bundle_list = pool.map(convert_to_bundle, json_file_dict[file_type])
        pool.map(upload_handler.upload_bundle, bundle_list)
