import unittest
from mock import patch, Mock
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
    @patch('ocr.BATCH_SIZE', 3)
    def test_returns_none_when_done(self, fetch_resources):
        data = ['first_data', 'second_data', 'third_data']
        fetch_resources.return_value = iter(data)
        # first call should return 3 items
        first_call = self.stream.read()
        # second call should not return items
        second_call = self.stream.read()
        # first batch should contain all data
        self.assertEqual(first_call, data)
        # second batch should be empty
        self.assertIsNone(second_call)

    # Raises exception if response is not a csv
    @patch('ocr.urllib2.urlopen')
    def test_raises_for_non_csv(self, mock_urlopen):
        err = 'ERROR - Non CSV response'
        with self.assertRaisesRegexp(Exception, err):
            self.stream._api_call(None)

    # Handles unencodable strings
    @patch('ocr.urllib2.urlopen')
    def test_hanldes_unencodable_strings(self, mock_urlopen):
        res_mock = Mock()
        res_mock.info.side_effect = self.foo
        # The characters here represent a copywrite symbol
        res_mock.read.side_effect = lambda: "{'problem_text': 'OHNO!\xc2\xae'}"
        mock_urlopen.return_value = res_mock

        exception_raised = False
        try:
            self.stream.read()
        except Exception, e:
            exception_raised = e

        self.assertFalse(exception_raised, exception_raised)

    def foo(self):
        return {'content-type': 'csv'}

    # Sets resource to None once no more batches are left
    def test_extract_batch(self):
        l = ['v1', 'v2']
        self.stream._extract_batch(iter(l), None)
        self.assertIsNone(self.stream.resource)

    # Increases process count when processing resources
    @patch('ocr.OcrSource._fetch_resource')
    @patch('ocr.BATCH_SIZE', 3)
    def test_increases_processed_count(self, fetch_resources):
        self.source['resources'].append(
            {'name': 'another resource', 'value': 'another value'}
        )
        self.stream = OcrSource(self.source, OPTIONS)
        data = ['first_data', 'second_data', 'third_data']
        fetch_resources.return_value = iter(data)

        # Nothing processed at the begining
        self.assertEqual(self.stream.processed, 0)

        # First read should return all data in one batch
        self.stream.read()
        # Processed one resource
        self.assertEqual(self.stream.processed, 1)

        # Second read should move to next resource
        self.stream.read()
        # Processed two resource
        self.assertEqual(self.stream.processed, 2)


# Run the test suite
if __name__ == '__main__':
    unittest.main()
