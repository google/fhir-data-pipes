import unittest
import resources
import json 

class ResourcesTest(unittest.TestCase):
    
  def _read_file(self, resource):
   with open(f'test_files/test_{resource}.json') as f:
       return json.loads(f.read())

  def setUp(self):
    super().setUp()
    self.patient_data = self._read_file('patient')
    self.encounter_data = self._read_file('encounter')
    self.observation_data = self._read_file('observation')

  def test_openmrs_convert_patient(self):
      test_patient = resources.Patient(self.patient_data)
      self.assertEqual(test_patient.original_id, '2a76c0e3-1e73-2109-3d5e-8a8871fe35d7')
      self.assertEqual(test_patient.json['identifier'], [])
      test_patient.openmrs_convert()
      self.assertEqual(test_patient.json['identifier'], [{
        'extension': [{
            'url': 'http://fhir.openmrs.org/ext/patient/identifier#location',
            'valueReference': {
                'reference': 'Location/8d6c993e-c2cc-11de-8d13-0010c6dffd0f',
                'type': 'Location',
                'display': 'Unknown Location'
            }
        }],
        'use': 'official',
        'type': {
            'text': 'OpenMRS ID'
        },
        'value': "21763053157321093455818871653547C",
        'id': test_patient.original_id
    }])

  def test_openmrs_convert_encounter(self):     
    test_encounter = resources.Encounter(self.encounter_data)
    self.assertEqual(test_encounter.original_id, 'e1846945-43bb-3cfe-1faf-66cf99935c7c')

    test_encounter.openmrs_convert('123')
    self.assertEqual(test_encounter.json['type'], [{
        'coding': [{
            'system': 'http://fhir.openmrs.org/code-system/encounter-type',
            'code': '5021b1a1-e7f6-44b4-ba02-da2f2bcf8718',
            'display': 'Attachment Upload'
        }]
    }] )
    self.assertEqual(test_encounter.json['subject']['reference'] , 'Patient/123') 

    with self.assertRaises(KeyError) as error:
      test_encounter.json['identifier']
      test_encounter.json['participant']


  def test_openmrs_convert_observation(self):    
    test_observation = resources.Observation(self.observation_data)
    self.assertEqual(test_observation.original_id, 'e0f03934-1b25-6f93-e437-b554af10675e')

    encounter = resources.Encounter(self.encounter_data)
    encounter_list = [encounter]
    with self.assertRaises(TypeError) as error:
        test_observation.openmrs_convert("123", encounter_list)
    
    encounter.new_id = "5678"
    test_observation.openmrs_convert("123", encounter_list)
    
    self.assertEqual(test_observation.json['subject']['reference'] , 'Patient/123')   
    self.assertEqual(test_observation.json['encounter']['reference'] , 'Encounter/5678')
    
    with self.assertRaises(KeyError) as error:
      test_observation.json['code']['coding'][0]['system']
      test_observation.json['valueCodeableConcept']['coding'][0]['system']
    



        
        