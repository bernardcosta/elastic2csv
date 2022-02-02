import json
import os, shutil
from datetime import datetime
import logging
import sys
import traceback
log = logging.getLogger(__name__)


def mkdir(rel_path):
    # Check whether the specified path exists or not
    if not os.path.exists(rel_path):
      # Create a new directory because it does not exist
      os.makedirs(rel_path)
      log.info(f'New Directory Created: {rel_path}')
    else:
        log.info(f'Output Directory already created.')

def connect_elasticsearch():
    log.info("Connected to elasticsearch server")
    log.info(str(sys.argv[1]))
    return Elasticsearch([os.environ["ESSERVER"]], timeout=500)

def search_and_export(es_instance, query, out_dir):
    outfile = os.path.join(out_dir,f'dump{str(datetime.now()).replace(" ","")}.json')
    log.info(f'dumping data to {outfile}')
    with open(outfile, 'a+') as out:
        count = 1
        while True:
            res = es_instance.search(index=str(os.environ['INDEX']), query=query["query"], aggs=query["aggs"])

            out.write(json.dumps(res['aggregations']['two']['buckets']))
            out.write("\n")
            if "after_key" not in res['aggregations']['two']:
                break

            after_dict = {"split":res['aggregations']['two']['after_key']['split']}
            log.info(f'p{count} - {count * 700}: {after_dict}')
            query['aggs']['two']["composite"]["after"] = after_dict
            count = count + 1

    return outfile

def load_request(directory):
    with open(str(directory), encoding='utf-8') as f:
        request = json.loads(f.read())
        log.info(f'Reading request file \n {request}')
        return request
