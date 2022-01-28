from elasticsearch import Elasticsearch
import json
import os, shutil
from datetime import datetime
import logging
import sys
import traceback
log = logging.getLogger(__name__)
from dotenv import load_dotenv


def mkdir(rel_path):
    # Check whether the specified path exists or not
    if not os.path.exists(rel_path):
      # Create a new directory because it does not exist
      os.makedirs(rel_path)
      log.info(f'New Directory Created: {rel_path}')
    else:
        log.info(f'Output Directory already created.')


def dump_response(es_instance, query, out_dir):
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

def load_request():
    with open(str(sys.argv[1]), encoding='utf-8') as f:
        request = json.loads(f.read())
        log.info(f'Reading request file \n {request}')
        return request


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    log.info(f'Starting export')
    log.info(sys.argv[1])
    log.info(len(sys.argv))

    try:
        mkdir("out")

        es = Elasticsearch([os.environ["ESSERVER"]], timeout=500)
        log.info("Connected to elasticsearch server")
        log.info(str(sys.argv[1]))

        req = load_request()
        log.info('dumping response')

        output_file = dump_response(es, req, "out")
        # make a copy to root directory for further pipeline manipulation
        shutil.copy(output_file, 'tmp_dump.json')

    except Exception as e:
        log.exception("compositeexport.py")
