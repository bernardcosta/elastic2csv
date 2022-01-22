from elasticsearch import Elasticsearch
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
        log.info(f'Path already exists. Will overwrite output')


def dump_response(es_instance, query, output_file):
    with open(os.path.join(output_file,'dump.json'), 'a+') as out:
        while True:
            res = es_instance.search(index=str(os.environ['INDEX']), body=query, headers={"accept": "application/vnd.elasticsearch+json; compatible-with=7"})

            out.write(json.dumps(res['aggregations']['two']['buckets']))
            out.write("\n")
            if "after_key" not in res['aggregations']['two']:
                break

            after_dict = {"urls":res['aggregations']['two']['after_key']['urls']}
            query['aggs']['two']["composite"]["after"] = after_dict

def load_request():
    with open(str(sys.argv[1]), encoding='utf-8') as f:
        request = json.loads(f.read())
        log.info('Reading request file')
        return request


if __name__ == "__main__":

    logging.basicConfig(filename=f'std.log', level=logging.INFO, filemode='w', format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    log.info(f'Starting export')
    log.info(sys.argv[1])
    log.info(len(sys.argv))

    try:
        output_path = os.path.join("out",f"{sys.argv[2]}-{str(datetime.now()).replace(' ','')}")
        mkdir(output_path)

        es = Elasticsearch([os.environ["PFBISERVER"]])
        log.info("Connected to elasticsearch server")
        log.info(str(sys.argv[1]))

        req = load_request()

        dump_response(es, req, output_path)
        # make a copy to root directory for further pipeline manipulation
        shutil.copy(os.path.join(output_path,'dump.json'), 'tmp_dump.json')

    except Exception as e:
        log.exception("compositeexport.py")
