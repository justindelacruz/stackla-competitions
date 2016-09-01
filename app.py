import csv
import sys
import getopt
import urllib
import json
from datetime import datetime
import glob
import re
import hashlib

class StacklaCompetitions:
    def __init__(self):
        self.access_token = 'INSERT ACCESS TOKEN HERE' # todo: get this from environment
        self.stack = 'STACK_NAME'

    def fetch_api_data(self, filter_id):
        """ Fetch filter data from the Stackla API.

        :param filter_id:
        :return: JSON object
        """
        url = 'https://api.stackla.com/api/filters/{0}/content?page=1&limit=10&status=published%2Cqueued%2Cdisabled&stack={2}&access_token={1}'.format(filter_id, self.access_token, self.stack)
        response = urllib.urlopen(url)

        return json.loads(response.read())

    def write_to_file(self, json_data):
        """
        Fetch
        :param data:
        :return:
        """
        timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        filename = timestamp + '.json'
        with open('data/' + filename, 'w') as f:
            json.dump(json_data, f, indent=4, separators=(',', ': '), encoding='utf-8')
        # print "Data written to {0}".format(filename)

    def write_csv(self):
        hash = None
        files = glob.glob("data/*.json")
        with open('Competition Results.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter='|', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
            writer.writerow([
                "Stackla ID",
                "Votes",
                "Source",
                "Content",
                "Date Created (UTC)",
                "URL"
            ])
            writer.writerow([])

            for file in files:
                with open(file) as f:
                    json_data = json.load(f, encoding="utf-8")
                    data = json_data['data']

                    rows = []
                    for item in data:

                        if item['numVotes'] > 0:
                            if 'original_url' in item:
                                url = item['original_url']
                            elif 'original_link' in item:
                                url = item['original_link']
                            else:
                                url = ''

                            rows.append([
                                item['_id'],
                                item['numVotes'],
                                item['source'],
                                item['message'].encode('utf-8').strip(),
                                item['created'],
                                url.strip()
                            ])

                    date = re.search(r'data/(.*).json', file).group(1)
                    new_hash = hashlib.sha1(str(rows)).hexdigest()
                    if hash == new_hash:
                        # print "Read " + file + " but the hash as the same as before: " + new_hash
                        writer.writerow(["Results from " + date + " skipped (no change)"])
                    else:
                        hash = new_hash
                        writer.writerow(["Results from " + date])
                        writer.writerows(rows)

                    writer.writerows([''])


def main(argv):
    # Parse command-line arguments
    try:
        opts, args = getopt.getopt(argv, "fw", ["filter=", "write"])
    except getopt.GetoptError:
        sys.exit(2)

    filter_id = False
    write = False

    for opt, arg in opts:
        if opt in ("-f", "--filter"):
            filter_id = arg
        if opt in ("-w", "--write"):
            write = True
            # print("Write flag is enabled.")

    # Validate arguments
    if not filter_id:
        raise Exception('Error: Must define filter ID (-f or --filter).')

    stackla_competitions = StacklaCompetitions()
    json_data = stackla_competitions.fetch_api_data(filter_id)
    stackla_competitions.write_to_file(json_data)

    if write:
        stackla_competitions.write_csv()

if __name__ == "__main__":
    main(sys.argv[1:])
