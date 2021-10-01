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

from typing import List, Type, Union

import common
import multi_answer_mod
import one_answer_mod
import pandas as pd


def create_submodules_list(
    csv_file: str,
    submodule_type: Union[Type[multi_answer_mod.MultiAnswerSubmodule],
                          Type[one_answer_mod.OneAnswerSubmodule]]
) -> List[Union[multi_answer_mod.MultiAnswerSubmodule,
                one_answer_mod.OneAnswerSubmodule]]:
  """Creates a list containing submodules that need to be created.

  Reads the CSV as a Dataframe, creates a submodule object per question, and
  appends the possbile answers.

  Args:
    directory: location of csv file to read from
    submodule_type: either a one answer, or multi answer submodule 

  Returns:
    List of submodule objects.
  """

  submod_list = []
  df = pd.read_csv(csv_file)
  for _, df in df.groupby('QUESTION FROM AMPATH'):
    if pd.notna(df.iloc[0]['CIEL EQUIVALENT']):
      t = submodule_type(
          df.iloc[0]['QUESTION FROM AMPATH'],
          common.CodeDisplay(df.iloc[0]['QUESTION FROM AMPATH'],
                             df.iloc[0]['CIEL EQUIVALENT']))
      submod_list.append(t)

      for _, row in df.iterrows():
        if pd.notna(row['CIEL CODE ANSWER']):
          t.add_answer(
              common.CodeDisplay(row['CODED ANSWERS FROM OPENMRS'],
                                 row['CIEL CODE ANSWER']))
  return submod_list


if __name__ == '__main__':
  submodules_list = create_submodules_list('ampath-q-a.csv',
                                           one_answer_mod.OneAnswerSubmodule)
  for submodule in submodules_list:
    submodule.fill_distributed_transition()
    submodule.loop_through_answers()
    submodule.save()

  submodules_list = create_submodules_list(
      'ampath-q-a-multi.csv', multi_answer_mod.MultiAnswerSubmodule)
  for submodule in submodules_list:
    submodule.loop_through_answers()
    submodule.save()
