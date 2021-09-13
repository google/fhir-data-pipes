import unittest
from unittest import mock

import uploader


class UploaderTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_hook = mock.MagicMock()
    self.mock_bundle = mock.MagicMock()
    self._upload_resource = mock.patch.object(
        uploader.Uploader, '_upload_resource', return_value='123').start()

  def test_upload_bundle(self):
    self.mock_bundle.openmrs_patient = mock.MagicMock()
    upload_handler = uploader.Uploader(self.mock_hook)
    upload_handler.upload_openmrs_bundle(self.mock_bundle)
    self.assertTrue(self._upload_resource.called)
    self.assertEqual(self.mock_bundle.openmrs_patient.new_id, '123')

  def test_upload_bundle_gcp(self):
    self.mock_bundle.patient = None
    upload_handler = uploader.Uploader(self.mock_hook)
    upload_handler.upload_bundle(self.mock_bundle)
    self.assertFalse(self._upload_resource.called)
