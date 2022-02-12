from elasticsearch import Elasticsearch
import progressbar
import logging
log = logging.getLogger(__name__)
import json
import os

class Elastic2csv:

    def __init__(self, arguments, scroll_time='1m', timeout=180):
        self.args = arguments
        self.scroll = scroll_time
        self.scroll_ids = []
        self.timeout = timeout
        self.connection = None
        self.query = None


    #TODO: retry connections
    def connect(self):
        es = Elasticsearch(self.args.url, timeout=self.timeout)
        self.connection = es

    def search(self):
        self.load_request_file()
        res = self.connection.search(index=str(self.args.index)+"-*", query=self.query["query"], aggs=self.query["aggs"], scroll=self.scroll)
        num_results = res['hits']['total']['value']
        log.info(f'Total Results {num_results}')
        scroll_id = res["_scroll_id"]

        total_hits = 0

        widgets = ['Run query ',
                       progressbar.Bar(left='[', marker='#', right=']'),
                       progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
                       progressbar.Percentage(),
                       progressbar.FormatLabel('] [%(elapsed)s] ['),
                       progressbar.ETA(), '] [',
                       progressbar.FileTransferSpeed(unit='docs'), ']'
                       ]
        bar = progressbar.ProgressBar(widgets=widgets, maxval=num_results).start()

        while total_hits != num_results:
            total_hits += 1
            if res['_scroll_id'] not in self.scroll_ids:
                self.scroll_ids.append(res["_scroll_id"])

            if not res['hits']['hits']:
                break

            for hit in res['hits']['hits']:
                 # out.write(json.dumps(res['aggregations'][split_key]['buckets']))
                 # out.write("\n")
                 bar.update(total_lines)



                 res = self.connection.scroll(scroll=self.scroll, scroll_id=res["_scroll_id"])


    def clean_scroll_ids(self):
        try:
            if self.scroll_ids:
                self.es_conn.clear_scroll(body=','.join(self.scroll_ids))
            else:
                log.info("no scroll ids")
        except:
            pass


    def load_request_file(self):
        with open(str(self.args.request_file), encoding='utf-8') as f:
            request = json.loads(f.read())
            log.info(f'Loading {self.args.request_file} - Size {os.path.getsize(self.args.request_file) / 1000}Kb')
            self.query = request
