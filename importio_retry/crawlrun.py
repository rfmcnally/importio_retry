""" Module for interacting with Import.io Crawl Runs """
import os
import pandas as pd

class CrawlRun(object):
    """ Class for handling Import.io Crawl Runs """
    def __init__(self, crawl_id, extractor_id=None, csv_id=None, log_id=None):
        self.crawl_id = crawl_id
        self.extractor_id = extractor_id
        self.csv_id = csv_id
        self.log_id = log_id
        self._apikey = os.environ['IMPORT_IO_API_KEY']

    def get_results_df(self):
        """ Returns a crawl run CSV as a DataFrame """
        url = 'https://store.import.io/crawlrun/{0}/_attachment/csv/{1}?_apikey={2}'.format(
            self.crawl_id, self.csv_id, self._apikey)
        results = pd.read_csv(url, keep_default_na=False, index_col=False)
        return results

    def get_log_df(self):
        """ Returns a crawl run log as  a DataFrame """
        url = 'https://store.import.io/crawlrun/{0}/_attachment/log/{1}?_apikey={2}'.format(
            self.crawl_id, self.log_id, self._apikey)
        log = pd.read_csv(url, keep_default_na=False, index_col=False)
        return log
