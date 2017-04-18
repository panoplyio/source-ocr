import urllib
import urllib2
import csv
from tempfile import SpooledTemporaryFile
import panoply


BASE_URL = 'https://api.oneclickretail.com'
DESTINATION = 'ocr_reports_csv'
IDPATTERN = '{week_asin}'
BATCH_SIZE = 200
FETCH_META = False
DEFAULT_WEEKS_BACK = 1
MAX_SIZE = 100 * (1024 * 1024)  # 100mb


class OcrSource(panoply.DataSource):
    """
    One Click Retail data source.
    API Docs (require login) - https://api.oneclickretail.com/api-docs-login
    Implemented endpoints -
    GET /v3/clients/{client_uuid}/reports/csv
    """

    def __init__(self, source, options):
        super(OcrSource, self).__init__(source, options)

        if not source.get('destination'):
            source['destination'] = DESTINATION

        if not source.get('idpattern'):
            source['idpattern'] = IDPATTERN

        if not source.get('resources'):
            raise Exception('No resources selected')

        self.resource = None
        self.data = None
        self.resources = source.get('resources')
        self.api_key = source.get('apiKey')
        self.weeks = source.get('weeks', DEFAULT_WEEKS_BACK)
        self.clientUUID = source.get('clientUUID')
        self.processed = 0
        self.total = len(self.resources)

    def read(self, batch_size=None):
        try:
            if not self.resource:
                self.resource = self.resources.pop()
                self.processed += 1
        except IndexError:
            # IndexError will occur when attempting to pop and element
            # off an empty list. This indicates that we have already
            # processed all resources.
            return None

        # Send progress message
        progress_msg = 'Fetching data for %s' % self.resource.get('name')
        self.progress(self.processed, self.total, progress_msg)

        # If data remains from the last resource, use it.
        # Otherwise fetch data from the current resource.
        self.data = self.data or self._fetch_resource()

        batch = self._extract_batch(self.data, batch_size)

        # If no data was returned for this batch we might have reached
        # the end of the source - attempt to read more.
        if not batch:
            # This was the last batch for this resource
            self.resource = None
            # Attempt to fetch the next resouce
            return self.read()

        return batch

    def _fetch_resource(self):
        """
        Assemble the api call, execute it and parse the
        csv response as a list of dicts. Returns a dict generator
        """

        qs = self._build_qs()  # Build the query string
        url = self._build_url(qs)  # Build the full url
        fp = self._api_call(url)  # Fetch the data as a file pointer

        # Parse the list of dicts in to a dict generator
        return csv.DictReader(fp)

    def _build_qs(self):
        params = {
            'meta': FETCH_META,
            'X-API-KEY': self.api_key,
            'weeks_back': self.weeks
        }

        qs = urllib.urlencode(params)
        return qs

    def _build_url(self, qs):
        base = self.resource.get('value') % self.clientUUID
        url = '%(base)s/%(endpoint)s?%(qs)s' % {
            'base': BASE_URL,
            'endpoint': base,
            'qs': qs
        }
        return url

    def _api_call(self, url):
        """
        Returns a SpooledTemporaryFile - this is a file that is initially
        stored in memory but once its size exceedes max_size it will start
        writing to disk. It is used because there is no way of knowing how
        large of a file the api will return.
        We are expecting a csv file.
        """
        self.log('Request URL', url)
        response = urllib2.urlopen(url)
        self.log('Got response', response)

        # the response MUST be a csv file
        content_type = response.info().get('content-type')
        if 'csv' not in content_type:
            raise Exception('ERROR - Non CSV response.')

        raw_data = response.read()
        # Decode the returned data and replace any characters that generate
        # encoding errors with the default unicode replacement character.
        data = raw_data.decode('utf-8', 'replace')

        self.tmp_file = SpooledTemporaryFile(max_size=MAX_SIZE)
        # Force writing the data encoded as utf-8. If we don't do this,
        # python will attempt to write the data as 'ascii' which does not
        # support special characters

        self.tmp_file.write(data.encode('utf-8'))

        # 'rewind' the file pointer in order to
        # read it back durring `_extract_batch()`
        self.tmp_file.seek(0)

        return self.tmp_file

    def _extract_batch(self, data, batch_size):
        """
        Iterates over BATCH_SIZE of data
        returning a list or results
        """

        batch_size = batch_size or BATCH_SIZE

        batch = []
        try:
            for i in range(batch_size):
                batch.append(data.next())
        except StopIteration:
            pass

        return batch
