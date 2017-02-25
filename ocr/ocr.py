import urllib2
import csv
from tempfile import SpooledTemporaryFile
from datetime import datetime, date
import panoply


BASE_URL = 'https://api.oneclickretail.com'
DESTINATION = 'ocr_reports_csv'
IDPATTERN = '{week_asin}'
BATCH_SIZE = 200
FETCH_META = False
DEFAULT_WEEKS = 1
DATE_FORMAT = '%Y-%m-%d'
MAX_SIZE = 104857600  # 100mb


class OcrSource(panoply.DataSource):
    """
    One Click Retail data source.
    API Docs (require login) - https://api.oneclickretail.com/api-docs-login
    Implemented endpoints -
    GET /v3/clients/{client_uuid}/reports/csv
    """

    resources = None
    resource = None
    tmp_file = None
    data = []
    last_run = None

    api_key = None
    weeks = None
    clientUUID = None

    def __init__(self, source, options):
        super(OcrSource, self).__init__(source, options)

        if not source.get('destination'):
                source['destination'] = DESTINATION

        if not source.get('idpattern'):
                source['idpattern'] = IDPATTERN

        if not source.get('resources'):
            raise Exception('No resources selected')

        self.resources = source.get('resources')
        self.api_key = source.get('apiKey')
        self.weeks = source.get('weeks', DEFAULT_WEEKS)
        self.clientUUID = source.get('clientUUID')

        self.last_run = self._get_last_run(source.get('lastTimeSucceed'))

    def read(self):
        try:
            if not self.resource:
                self.resource = self.resources.pop()
        except IndexError:
            # IndexError will occur when attempting to pop and element
            # off an empty list. This indicates that we have already
            # processed all resources.
            return None

        # If data remains from the last resource, use it.
        # Otherwise fetch data from the current resource.
        self.data = self.data or self._fetch_resource(self.resource)

        batch = self._extract_batch(self.data)

        # If no data was returned for this batch we might have reached
        # the end of the source - attempt to read more.
        if not batch:
            # This was the last batch for this resource
            self.resource = None
            return self.read()

        return batch

    def _extract_batch(self, data):
        """
        Iterates over BATCH_SIZE of data
        returning a list or results
        """
        batch = []
        try:
            for i in range(BATCH_SIZE):
                batch.append(data.next())
        except StopIteration:
            pass

        return batch

    def _fetch_resource(self, resource):
        """
        Assemble the api call, execute it and parse the
        csv response as a list of dicts
        """

        progress_msg = 'Fetching data for %s' % resource.get('name')
        self.progress(None, None, progress_msg)

        qs = self._build_qs() # Build the query string
        url = self._build_url(qs) # Build the full url
        fp = self._api_call(url) # Fetch the data as a file pointer
        reader = csv.DictReader(fp) # Parse the csv file to a dict generator
        return reader

    def _build_qs(self):
        qs = 'meta=%(meta)s&X-API-KEY=%(key)s&weeks_back=%(weeks)s' % {
            'meta': FETCH_META,
            'key': self.api_key,
            'weeks': self.weeks,
            'start_date': self.last_run
        }
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
        Uses a SpooledTemporaryFile - this is a file that is initially
        stored in memory but once its size exceedes max_size it will start
        writing to disk. It is used because there is no way of knowing how
        large of a file the api will return.
        """
        response = urllib2.urlopen(url)
        self.tmp_file = SpooledTemporaryFile(
            max_size = MAX_SIZE
        )
        self.tmp_file.write( response.read() )
        # 'rewind' the file pointer in order to
        # read it back durring `_extract_batch()`
        self.tmp_file.seek(0)

        return self.tmp_file

    def _get_last_run(self, ts_string = None):
        """
        Converts ts_string (ISO) to correct format if proivded.
        If not provided, use todays date
        """
        last_run = date.today().strftime(DATE_FORMAT)
        if ts_string:
            last_run = ts_string[:ts_string.index('T')]

        return last_run
