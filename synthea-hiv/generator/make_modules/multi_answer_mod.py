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
import random

import common


class MultiAnswerSubmodule:
  """Synthea Submodule for a question and its possible answers."""

  def __init__(self, name: str, question: common.CodeDisplay):
    self.name = name.lower().replace(',', '').replace(' ', '_')
    self.question = question
    self.answers = []
    self.base_template = {
        'name': self.name,
        'remarks': ['An HIV Submodule'],
        'states': {},
        'gmf_version': 2
    }

  def add_answer(self, answer: common.CodeDisplay):
    self.answers.append(answer)

  def call_answer_key(self, obs: common.CodeDisplay, state_name: str):
    self.base_template['states'][obs.name] = common.fill_answer_key(
        self.question, obs, state_name)

  def create_transition(self, obs: common.CodeDisplay, state_type: str,
                        state_num: int):
    probability = random.uniform(0, 1)
    if state_type == 'Initial' or state_type == 'Terminal':
      state_name = state_type
    else:
      state_name = f'{state_type}_{state_num}'

    self.base_template['states'][state_name] = {
        'type':
            state_type,
        'name':
            state_name,
        'distributed_transition': [{
            'transition': obs.name,
            'distribution': probability
        }, {
            'transition': f'Simple_{state_num+1}',
            'distribution': 1 - probability
        }]
    }

  def loop_through_answers(self):
    """Add each answer to the template."""
    answer = self.answers.pop()
    self.create_transition(answer, 'Initial', 0)
    state_num = 1
    self.call_answer_key(answer, f'Simple_{state_num}')

    while self.answers:
      answer = self.answers.pop()
      self.create_transition(answer, 'Simple', state_num)
      state_num += 1
      self.call_answer_key(answer, f'Simple_{state_num}')

    self.base_template['states'][f'Simple_{state_num}'] = {
        'type': 'Terminal',
        'name': f'Simple_{state_num}'
        }

  def save(self):
    """Save as a JSON file."""
    with open(f'../hiv_simple/{self.name}.json', 'w') as f:
      json.dump(self.base_template, f)

  def __str__(self):
    return json.dumps(self.base_template)

  def __repr__(self):
    return self.__str__() 
