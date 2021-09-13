"""Helper functions to generate Checksum for OpenMRS uploads."""


def luhn_id_generator(
    num: int, base_chars: str = '0123456789ACDEFGHJKLMNPRTUVWXY') -> str:
  """Takes in a number and returns the number with the checksum."""

  num = str(num)
  base_length = len(base_chars)
  factor = 2
  summation = 0

  for i in range(len(num) - 1, -1, -1):
    code_point = base_chars.index(num[i])
    addend = factor * code_point
    factor = 1 if (factor == 2) else 2
    addend = (addend // base_length) + (addend % base_length)
    summation += addend

  remainder = summation % base_length
  check_code_point = (base_length - remainder) % base_length
  num += base_chars[check_code_point]
  return num

def convert_to_int(original_id: str):
  """Converts alphanumeric  string to  string of numbers only."""

  original_id = original_id.replace('-', '')

  temp_list = []
  for char in original_id.lower():
    if char.isalpha():
      number = ord(char) - 96
      temp_list.append(str(number))
    else:
      temp_list.append(char)

  return ''.join(temp_list)