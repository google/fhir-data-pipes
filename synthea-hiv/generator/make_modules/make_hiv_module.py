"""Creates Synthea Submodules from CSV file."""

import json
from typing import Dict, List

import numpy as np
import pandas as pd


class CodeDisplay:
  """Holds name and code value."""

  def __init__(self, name: str, code: str):
    self.name = name
    self.code = code

  def __str__(self):
    return self.name

  def __repr__(self):
    return self.__str__()


class Question(CodeDisplay):
  """Wrapper for Questions."""
  pass


class Answer(CodeDisplay):
  """Wrapper for Answers."""
  pass


class Submodule:
  """Synthea Submodule for a question and its possible answers."""

  def __init__(self, name: str, question: Question):
    self.name = name.lower().replace(',', '').replace(' ', '_')
    self.question = question
    self.answers = []
    self.base_template = {
        'name': self.name,
        'remarks': ['An HIV Submodule'],
        'states': {
            'Initial': {
                'type': 'Initial',
                'name': 'Initial',
                'distributed_transition': None
            },
            'Terminal': {
                'type': 'Terminal',
                'name': 'Terminal'
            }
        },
        'gmf_version': 2
    }

  def add_answer(self, answer: Answer):
    self.answers.append(answer)

  def fill_distributed_transition(self):
    """Assigns a probability to each answer being selected."""

    disribution = np.random.dirichlet(
        np.ones(len(self.answers)), size=1).tolist()[0]
    self.base_template['states']['Initial']['distributed_transition'] = list(
        map(self._map_dist, self.answers, disribution))

  def _map_dist(self, ans: List[Answer], dist: List[float]) -> Dict[str, float]:
    """Helper function to map answers to probability."""

    return {'transition': ans.name, 'distribution': dist}

  def fill_answer_key(self):
    """Add each answer to the template."""

    for answer in self.answers:
      self.base_template['states'][answer.name] = {
          'type': 'Observation',
          'category': 'survey',
          'unit': '',
          'codes': [{
              'system': 'LOINC',
              'code': self.question.code,
              'display': self.question.name,
              'value_set': ''
          }],
          'direct_transition': 'Terminal',
          'name': answer.name,
          'value_code': {
              'system': 'SNOMED-CT',
              'code': answer.code,
              'display': answer.name
          }
      }

  def save(self):
    """Save as a JSON file."""

    with open(f'generator/hiv_simple/{self.name}.json', 'w') as f:
      json.dump(self.base_template, f)

  def __str__(self):
    return json.dumps(self.base_template)

  def __repr__(self):
    return self.__str__()


def create_submodules_list(
    csv_file: str = 'generator/make_modules/ampath-q-a.csv') -> List[Submodule]:
  """Creates a list containing Submodule objects.

  Reads the CSV as a Dataframe, creates a Submodule object per question, and
  appends the possbile answers.

  Args:
    directory: location of csv file to read from

  Returns:
    List of Submodule objects.
  """

  submod_list = []
  df = pd.read_csv(csv_file)
  for _, df in df.groupby('QUESTION FROM AMPATH'):
    if pd.notna(df.iloc[0]['CIEL EQUIVALENT']):
      t = Submodule(
          df.iloc[0]['QUESTION FROM AMPATH'],
          Question(df.iloc[0]['QUESTION FROM AMPATH'],
                   df.iloc[0]['CIEL EQUIVALENT']))
      submod_list.append(t)

      for _, row in df.iterrows():
        if pd.notna(row['CIEL CODE ANSWER']):
          t.add_answer(
              Answer(row['CODED ANSWERS FROM OPENMRS'],
                     row['CIEL CODE ANSWER']))
  return submod_list


if __name__ == '__main__':
  submodules_list = create_submodules_list()
  for submodule in submodules_list:
    submodule.fill_distributed_transition()
    submodule.fill_answer_key()
    submodule.save()