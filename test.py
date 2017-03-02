import unittest
from mock import patch
from ocr import (
    OcrSource,
    IDPATTERN,
    DESTINATION,
    DEFAULT_WEEKS_BACK
)

OPTIONS = {
    'logger': lambda *msgs: None,  # no-op logger
}


class TestOneClickRetail(unittest.TestCase):

    source = None
    stream = None

    def setUp(self):
        self.source = {
            'clientUUID': 'testUUID',
            'apiKey': 'testKey',
            'weeks': 1,
            'resources': [
                {'name': 'reports csv', 'value': 'v3/clients/%s/reports/csv'}
            ]
        }
        self.stream = OcrSource(self.source, OPTIONS)

    def tearDown(self):
        self.source = None

    # Ensures default IDPATTERN, DESTINATION, and DEFAULT_WEEKS are set
    def test_defaults(self):
        self.assertEqual(self.source['idpattern'], IDPATTERN)
        self.assertEqual(self.source['destination'], DESTINATION)
        self.assertEqual(self.source['weeks'], DEFAULT_WEEKS_BACK)

    # Raises exception if no resources are provided
    def test_resources_required(self):
        del self.source['resources']
        self.assertRaises(Exception, OcrSource, self.source, OPTIONS)

    # Return none when no more resources are left
    @patch('ocr.OcrSource._fetch_resource')
    @patch('ocr.ocr.BATCH_SIZE', 3)
    def test_returns_none_when_done(self, fetch_resources):
        data = ['first_data', 'second_data', 'third_data']
        fetch_resources.return_value = iter(data)
        # First call should return 3 items
        first_call = self.stream.read()
        # Second call should not return items
        second_call = self.stream.read()
        # First batch should contain all data
        self.assertEqual(first_call, data)
        # Second batch should be empty
        self.assertIsNone(second_call)

    # Raises exception if response is not a csv
    @patch('ocr.urllib2.urlopen')
    def test_raises_for_non_csv(self, mock_urlopen):
        err = 'ERROR - Non CSV response'
        with self.assertRaisesRegexp(Exception, err):
            self.stream._api_call(None)

    # Sets resource to None once no more batches are left
    def test_extract_batch(self):
        l = ['v1', 'v2']
        self.stream._extract_batch(iter(l))
        self.assertIsNone(self.stream.resource)


# Run the test suite
if __name__ == '__main__':
    unittest.main()
