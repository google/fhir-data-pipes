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

"""Creates Synthea Submodules from CSV file."""

import json
from typing import Dict, List

import common
import numpy as np


class OneAnswerSubmodule:
  """Synthea Submodule for a question and its possible answers."""

  def __init__(self, name: str, question: common.CodeDisplay):
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

  def add_answer(self, answer: common.CodeDisplay):
    self.answers.append(answer)

  def fill_distributed_transition(self):
    """Assigns a probability to each answer being selected."""

    distribution = np.random.dirichlet(
        np.ones(len(self.answers)), size=1).tolist()[0]
    self.base_template['states']['Initial']['distributed_transition'] = list(
        map(self._map_dist, self.answers, distribution))

  def _map_dist(self, ans: List[common.CodeDisplay],
                dist: List[float]) -> Dict[str, float]:
    """Helper function to map answers to probability."""

    return {'transition': ans.name, 'distribution': dist}

  def loop_through_answers(self):
    """Add each answer to the template."""

    for answer in self.answers:
      self.base_template['states'][answer.name] = common.fill_answer_key(
          self.question, answer, 'Terminal')

  def save(self):
    """Save as a JSON file."""

    with open(f'../hiv_simple/{self.name}.json', 'w') as f:
      json.dump(self.base_template, f)

  def __str__(self):
    return json.dumps(self.base_template)

  def __repr__(self):
    return self.__str__()
