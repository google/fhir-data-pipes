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
"""Helper functions to generate Checksum for OpenMRS uploads."""

_ASCII_OFFSET = 96
_BASE_CHARS = '0123456789ACDEFGHJKLMNPRTUVWXY'  # based on what OpenMRS uses:
# https://github.com/openmrs/openmrs-module-idgen/blob/master/api/src/main/java/org/openmrs/module/idgen/validator/LuhnMod30IdentifierValidator.java


def luhn_id_generator(num: int) -> str:
  """Takes in a number and returns the number with the checksum.

  Args:
    num: number to add checksum to end of

  Returns:
    String with appended checksum
  """
  num = str(num)
  base_length = len(_BASE_CHARS)
  factor = 2
  summation = 0

  for i in range(len(num) - 1, -1, -1):
    code_point = _BASE_CHARS.index(num[i])
    addend = factor * code_point
    factor = 1 if (factor == 2) else 2
    addend = (addend // base_length) + (addend % base_length)
    summation += addend

  remainder = summation % base_length
  check_code_point = (base_length - remainder) % base_length
  num += _BASE_CHARS[check_code_point]
  return num


def convert_to_int(original_id: str) -> str:
  """Converts the input alphanumeric string to a string of numbers only."""
  original_id = original_id.replace('-', '')
  temp_list = []
  for char in original_id.lower():
    if char.isalpha():
      number = ord(char) - _ASCII_OFFSET
      temp_list.append(str(number))
    else:
      temp_list.append(char)
  return ''.join(temp_list)
