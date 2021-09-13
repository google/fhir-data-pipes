import unittest
from unittest import mock
import fhir_hook
import log_mixin


def mock_requests_post(**kwargs):

  class MockResponse:

    def __init__(self, text, status_code):
      self.text = text
      self.status_code = status_code
      
      if status_code >= 400:
        self.request = mock.MagicMock()
        self.request.body  = "this happened"

  if 'localhost' in kwargs['url']:
    return MockResponse('{"resourceType":"Local"}', 200)
  elif 'google' in kwargs['url']:
    return MockResponse('{"resourceType":"Google"}', 201)

  return MockResponse(None, 404)


class FhirHookTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    mock.patch(
        'google.auth.transport.requests.Request',
        return_value=mock.MagicMock()).start()

    mock.patch(
        'google.auth.default', return_value=(mock.MagicMock(), '_')).start()

    mock.patch('requests.post', side_effect=mock_requests_post).start()

  def test_post_data_local(self):
    hook = fhir_hook.OpenMRSHook('http://localhost')
    hook.post_data(resource='Patient', data={'hi': 'mom'})
    self.assertEqual(hook.response, {'resourceType': 'Local'})

  def test_post_data_gcp(self):
    hook = fhir_hook.GcpHook('http://googleapis')
    hook.post_data(data={'hi': 'mom'})
    self.assertEqual(hook.response, {'resourceType': 'Google'})

  @mock.patch.object(log_mixin.LoggingMixin, 'logger')
  def test_post_data_error(self, mock_logger):
    hook = fhir_hook.OpenMRSHook('http://random')
    with self.assertRaises(ValueError):
        hook.post_data(data={'hi': 'mom'})

    mock_logger.debug.assert_called_with("this happened")

  def tearDown(self):
    super().tearDown()