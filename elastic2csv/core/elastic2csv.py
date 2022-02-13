from elasticsearch import Elasticsearch
import progressbar
import logging
log = logging.getLogger(__name__)
import json
import os
from datetime import datetime



class Elastic2csv:

    def __init__(self, arguments, scroll_time='1m', timeout=180):
        self.args = arguments
        self.scroll = scroll_time
        self.scroll_ids = []
        self.timeout = timeout
        self.connection = None
        self.query = None
        if self.args.server_username and self.args.server_host:
            self.port_forward()


    def port_forward(self):
        os.system("kill $( ps aux | grep '[9]201:' | awk '{print $2}' )")
        log.info("Using port 9201 to forward remote server to local.")
        os.system(f'ssh -f -N -q -L "9201:{self.args.url}" {self.args.server_username}@{self.args.server_host}')
        self.args.url="http://localhost:9201"



    #TODO: retry connections
    def connect(self):
        es = Elasticsearch(self.args.url, timeout=self.timeout)
        self.connection = es

    def search(self):
        self.load_request_file()
        # res = self.connection.search(index=str(self.args.index)+"-*", query=self.query["query"], aggs=self.query["aggs"])
        # num_results = res['hits']['total']['value']
        # log.info(f'Total Results {num_results}')
        # scroll_id = res["_scroll_id"]
        total_hits = 0

        # widgets = ['Run query ',
        #                progressbar.Bar(left='[', marker='#', right=']'),
        #                progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
        #                progressbar.Percentage(),
        #                progressbar.FormatLabel('] [%(elapsed)s] ['),
        #                progressbar.ETA(), '] [',
        #                progressbar.FileTransferSpeed(unit='docs'), ']'
        #                ]
        # bar = progressbar.ProgressBar(widgets=widgets, maxval=num_results).start()
        outfile = os.path.join(self.args.out_dir,f'dump{str(datetime.now()).replace(" ","")}.json')
        split_key = self.find_key(self.query)[-2]

        with open(outfile, 'a+') as out:
            count = 1
            while True:
                res = self.connection.search(index=str(self.args.index)+"-*", query=self.query["query"], aggs=self.query["aggs"])
                for hit in res['aggregations'][split_key]['buckets']:
                    total_hits += 1

                    out.write(json.dumps(hit))
                    out.write("\n")

                if "after_key" not in res['aggregations'][split_key]:
                    break

                after_dict = {"split":res['aggregations'][split_key]['after_key']['split']}
                log.info(f'p{count} - {count * 700}: {after_dict}')
                self.query['aggs'][split_key]["composite"]["after"] = after_dict
                count = count + 1


    def load_request_file(self):
        with open(str(self.args.request_file), encoding='utf-8') as f:
            request = json.loads(f.read())
            log.info(f'Loading {self.args.request_file} - Size {os.path.getsize(self.args.request_file) / 1000}Kb')
            self.query = request


    def find_key(self, d):
        for k,v in d.items():
            if isinstance(v, dict):
                p = self.find_key(v)
                if k == 'composite':
                    return [k]
                else:
                    return [k] + p
