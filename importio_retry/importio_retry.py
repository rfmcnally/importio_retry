""" Script for parsing out and retrying failed urls from Import.io Extractors """
import os
import sys
import argparse
import pandas as pd
from importioretry.extractor import Extractor
from importioretry.crawlrun import CrawlRun

def upload_failed_urls(retry_extractor):
    """ Function for parsing out and uploading failed urls. Takes extractors object as input.  """
    crawl_data = retry_extractor.latest_crawl()
    latest_crawl = CrawlRun(crawl_data['guid'], extractor_id=crawl_data['extractorId'],
                            log_id=crawl_data['log'])
    log = latest_crawl.get_log_df()
    fail_lines = log[log['Success'] == 0 | log['Message'].str.contains('No data extracted')]
    pd.set_option("display.max_colwidth", 10000)
    if not fail_lines.empty:
        fail_count = len(fail_lines)
        fail_urls = fail_lines['Url'].to_string(index=False)
        retry_extractor.put_url_body(fail_urls)
    else:
        fail_count = 0
    return fail_count

def retry_loop(retry_extractor, retries):
    """ Function for retrying extractor. Takes extractor object and number of retries as inputs. """
    retry_count = 0
    output_df = retry_extractor.get_df()
    while retry_count < retries:
        fail_count = upload_failed_urls(retry_extractor)
        if fail_count > 0:
            success = retry_extractor.run()
            if success:
                results_df = retry_extractor.get_df()
                output_df = output_df.append(results_df, ignore_index=True)
            retry_count += 1
        else:
            break
    return output_df

def extractor_retries(extractor_id, retries=2, run_first=False):
    """ Function for extractor retries. Takes extractor ID, run first, and retries as inputs. """
    if not os.environ['IMPORT_IO_API_KEY']:
        output = "Import.io API Key required as environment variable: IMPORT_IO_API_KEY."
        return output
    retry_extractor = Extractor(extractor_id)
    starting_urls = retry_extractor.get_url_body()
    if run_first:
        success = retry_extractor.run()
        if not success:
            output = "Initial run of {0} failed. Please check your extractor.".format(extractor_id)
            return output
    if retries == 0:
        fail_count = upload_failed_urls(retry_extractor)
        output = "{0} failed URLs uploaded to extractor {1}.".format(fail_count, extractor_id)
        return output
    else:
        output_df = retry_loop(retry_extractor, retries)
        output = output_df.to_csv(index=False)
        retry_extractor.put_url_body(starting_urls)
        return output

def parse_args():
    """ Function for parsing script arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument('extractor_id', help='Script requires an Import.io Extractor ID.')
    parser.add_argument('-r', '--retries', help='Number of retries to perform. Default = 2.',
                        type=int, default=2)
    parser.add_argument('-rF', '--run_first', help='Run extractor before retries. Default = False.',
                        type=bool, default=False)
    args = parser.parse_args()
    return args

def main():
    """ Runtime function """
    args = parse_args()
    output = extractor_retries(args.extractor_id, args.retries, args.run_first)
    sys.stdout.write(output)
    return

if __name__ == '__main__':
    main()
