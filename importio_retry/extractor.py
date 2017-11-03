""" Module for interacting with Import.io Extractors """
import os
import time
import requests
import pandas as pd

class Extractor(object):
    """ Class for handling Import.io Extractors """
    def __init__(self, extractor_id):
        self.extractor_id = extractor_id
        self._apikey = os.environ['IMPORT_IO_API_KEY']

    def start(self):
        """ Starts a crawl run on an extractor """
        url = "https://run.import.io/{0}/start".format(self.extractor_id)
        querystring = {"_apikey": self._apikey}
        headers = {'cache-control': "no-cache",}
        response = requests.post(url, headers=headers, params=querystring)
        if response.status_code == 200:
            result = response.json()['crawlRunId']
        else:
            result = response.json()
        return result

    def run(self):
        """ Starts a crawl run and polls the extractor until completion """
        run_id = self.start()
        result = False
        while True:
            state = self.status(run_id)
            if state == 'FINISHED':
                result = True
                break
            if state == 'CANCELLED':
                result = True
                break
            elif state == 'FAILED':
                break
            time.sleep(60)
        return result

    def get_url_body(self):
        """ Returns current URLs string body from an extractor """
        config_url = "https://store.import.io/extractor/{0}".format(self.extractor_id)
        querystring = {"_apikey": self._apikey}
        config_response = requests.get(config_url, params=querystring)
        config_data = config_response.json()
        url_id = config_data['urlList']
        url = "https://store.import.io/extractor/{0}/_attachment/urlList/{1}".format(
            self.extractor_id, url_id)
        response = requests.get(url, params=querystring)
        return response.text

    def put_url_list(self, urls):
        """ Uploads a list of URLs to an extractor """
        url = "https://store.import.io/extractor/{0}/_attachment/urlList".format(
            self.extractor_id)
        querystring = {"_apikey": self._apikey}
        headers = {'content-type': "text/plain"}
        data = "\n".join(urls)
        body = data.encode(encoding='utf-8')
        response = requests.put(url, data=body, headers=headers, params=querystring)
        return response

    def put_url_body(self, urls):
        """ Uploads a string body of URLs to an extractor """
        url = "https://store.import.io/extractor/{0}/_attachment/urlList".format(
            self.extractor_id)
        querystring = {"_apikey": self._apikey}
        headers = {'content-type': "text/plain"}
        response = requests.put(url, data=urls, headers=headers, params=querystring)
        return response

    def status(self, run_id):
        """ Returns the status of a crawl run """
        url = "https://store.import.io/crawlrun/{0}".format(run_id)
        querystring = {"_apikey": self._apikey}
        headers = {'Accept': "application/json"}
        response = requests.get(url, headers=headers, params=querystring)
        state = None
        try:
            results = response.json()
            try:
                state = results['state']
            except KeyError:
                pass
        except ValueError:
            pass
        return state

    def latest_crawl(self):
        """ Searches store for latest crawl run by an extractor and returns latest crawl run """
        url = 'https://store.import.io/crawlrun/_search'
        querystring = {'_sort': "_meta.creationTimestamp", '_page': "1", '_perpage': "1",
                       'extractorId': self.extractor_id, '_apikey': self._apikey}
        resp = requests.get(url, params=querystring)
        data = resp.json()
        latest_crawl = data['hits']['hits'][0]['fields']
        return latest_crawl

    def get_csv(self):
        """ Returns an extractor's latest CSV """
        url = "https://data.import.io/extractor/{0}/csv/latest".format(self.extractor_id)
        querystring = {"_apikey": self._apikey}
        headers = {'Accept': "text/csv"}
        response = requests.get(url, params=querystring, headers=headers, stream=True)
        csv_resp = response.content.decode('utf-8')
        return csv_resp.splitlines()

    def get_df(self):
        """ Returns an extractor's latest CSV as a DataFrame """
        url = "https://data.import.io/extractor/{0}/csv/latest?_apikey={1}".format(
            self.extractor_id, self._apikey)
        results = pd.read_csv(url, keep_default_na=False, index_col=False)
        return results
