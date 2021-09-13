import unittest
import idgen


class IdGenTest(unittest.TestCase):

  def test_luhn_id_generator(self):
    self.assertEqual('98056J', idgen.luhn_id_generator(98056))
    self.assertEqual('98056J', idgen.luhn_id_generator('98056'))

  def test_convert_to_int(self):
    self.assertEqual('89', idgen.convert_to_int('hi'))
    self.assertEqual('', idgen.convert_to_int(''))

    with self.assertRaises(AttributeError) as error:
      idgen.convert_to_int(123)


if __name__ == '__main__':
  unittest.main()
