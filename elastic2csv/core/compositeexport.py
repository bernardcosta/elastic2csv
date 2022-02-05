import json
import os
from datetime import datetime
import logging
import sys
log = logging.getLogger(__name__)
from elasticsearch import Elasticsearch


def mkdir(rel_path):
    # Check whether the specified path exists or not
    if not os.path.exists(rel_path):
      # Create a new directory because it does not exist
      os.makedirs(rel_path)
      log.info(f'New Directory Created: {rel_path}')

def connect_elasticsearch(host='localhost', port=9201):
    log.info("Connected to elasticsearch server")
    return Elasticsearch([f'{host}:{str(port)}'], timeout=500)


def find_key(d):
    for k,v in d.items():
        if isinstance(v, dict):
            p = find_key(v)
            if k == 'composite':
                return [k]
            else:
                return [k] + p


def search_and_export(es_instance, query, out_dir, index):
    outfile = os.path.join(out_dir,f'dump{str(datetime.now()).replace(" ","")}.json')
    log.info(f'dumping data to: {outfile}')
    split_key = find_key(query)[-2]
    log.info(f'split key: {split_key}')

    with open(outfile, 'a+') as out:
        count = 1
        while True:
            res = es_instance.search(index=str(index+"-*"), query=query["query"], aggs=query["aggs"])
            out.write(json.dumps(res['aggregations'][split_key]['buckets']))
            out.write("\n")
            if "after_key" not in res['aggregations'][split_key]:
                break

            after_dict = {"split":res['aggregations'][split_key]['after_key']['split']}
            log.info(f'p{count} - {count * 700}: {after_dict}')
            query['aggs'][split_key]["composite"]["after"] = after_dict
            count = count + 1

    return outfile

def load_request(directory):
    with open(str(directory), encoding='utf-8') as f:
        request = json.loads(f.read())
        log.info(f'Loading {directory} - Size {os.path.getsize(directory) / 1000}Kb')
        return request
