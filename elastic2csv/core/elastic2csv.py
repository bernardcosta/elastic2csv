#!/usr/bin/env python

from elasticsearch import Elasticsearch
import progressbar
import logging
log = logging.getLogger(__name__)
import json
import os
from datetime import datetime
import csv
import utils
from core import configurations as c


class Elastic2csv:

    def __init__(self, arguments, scroll_time='1m', timeout=180):
        self.args = arguments
        self.scroll = scroll_time
        self.scroll_ids = []
        self.timeout = timeout
        self.connection = None
        self.query = None
        self.outfile = os.path.join(self.args.out_dir,f'dump{str(datetime.now()).replace(" ","")}.json')
        if self.args.server_username and self.args.server_host:
            self.port_forward()


    def port_forward(self):
        command = f'ssh -f -N -q -L "9201:{self.args.url}" {self.args.server_username}@{self.args.server_host}'
        log.info(f'Port forwarding {command}')
        os.system("kill $( ps aux | grep '[9]201:' | awk '{print $2}' )")
        os.system(command)
        self.args.url="http://localhost:9201"
        log.info(f"New Elasticsearch URL {self.args.url}")


    #TODO: retry connections
    def connect(self):
        log.info(f"Connecting to elasticsearch: {self.args.url}")
        es = Elasticsearch(self.args.url, timeout=self.timeout)
        self.connection = es

    def search(self):
        log.info("Searching query...")
        total_hits = 0
        self.load_request_file()
        split_key = utils.find_key(self.query)[-2]
        c.COUNT_AGG["unique_count"]["cardinality"]["field"] = self.query["aggs"][split_key]["composite"]["sources"][0]["split"]["terms"]["field"]
        res = self.connection.search(index=str(self.args.index)+"-*", query=self.query["query"], aggs=c.COUNT_AGG, size = 0)
        max_hits = res['aggregations']['unique_count']['value']
        print(max_hits)
        widgets = ['Run query ',
                       progressbar.Bar(left='[', marker='#', right=']'),
                       progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
                       progressbar.Percentage(),
                       progressbar.FormatLabel('] [%(elapsed)s] ['),
                       progressbar.ETA(), '] [',
                       progressbar.FileTransferSpeed(unit='docs'), ']'
                       ]
        bar = progressbar.ProgressBar(widgets=widgets, maxval=max_hits).start()

        with open(self.outfile, 'a+') as out:
            count = 1
            out.write("[")
            while True:
                res = self.connection.search(index=str(self.args.index)+"-*", query=self.query["query"], aggs=self.query["aggs"])
                for hit in res['aggregations'][split_key]['buckets']:
                    if total_hits > 0:
                        out.write(",\n")
                    total_hits += 1
                    out.write(json.dumps(hit))
                    bar.update(total_hits)

                if "after_key" not in res['aggregations'][split_key]:
                    out.write("]")
                    break

                after_dict = {"split":res['aggregations'][split_key]['after_key']['split']}
                self.query['aggs'][split_key]["composite"]["after"] = after_dict
                count += 1
                log.info(f'Batch: {count} {total_hits} docs')

        self.to_csv()


    def load_request_file(self):
        with open(str(self.args.request_file), encoding='utf-8') as f:
            request = json.loads(f.read())
            log.info(f'Loading {self.args.request_file} - Size {os.path.getsize(self.args.request_file) / 1000}Kb')
            self.query = request

        log.info("  Loaded request body file.")

    def to_csv():
        data = json.load(open(self.outfile, 'r'))
        data = utils.flatten_json_list(data)

        with open(f"FinalOutput{str(datetime.now()).replace(' ','')}.csv","w") as f:
            cw = csv.DictWriter(f, c.COLUMNS, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, extrasaction='ignore')
            cw.writeheader()
            cw.writerows(data)
