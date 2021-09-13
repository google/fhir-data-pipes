import json
import tempfile
import unittest

import main


class MainTest(unittest.TestCase):

  def test_list_all_files(self):

    with tempfile.TemporaryDirectory() as tmpdirname:

      bundle_dic = main.list_all_files(tmpdirname)
      self.assertEqual(bundle_dic, {
          "practitioner": set(),
          "hospital": set(),
          "patient_history": set()
      })

      with tempfile.NamedTemporaryFile(suffix=".json", dir=tmpdirname):
        bundle_dic = main.list_all_files(tmpdirname)
      self.assertEqual(len(bundle_dic["patient_history"]), 1)

  def test_convert_to_bundle(self):
    config = {"resource": "patient", "name": {"first": "Super", "last": "man"}}
    temp_file = tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8")
    json.dump(config, temp_file)
    temp_file.flush()
    bundle = main.convert_to_bundle(temp_file.name)
    self.assertEqual(bundle.file_name, temp_file.name)