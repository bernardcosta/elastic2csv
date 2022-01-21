from elasticsearch import Elasticsearch
import json
import os
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
        with open(str(sys.argv[1]), encoding='utf-8') as f:
            query = json.loads(f.read())
            log.info('Reading request file')

        with open(os.path.join(output_path,'dump.json'), 'a+') as out:
            while True:
                res = es.search(index=str(os.environ['INDEX']), body=query)
                
                out.write(str(res['aggregations']['two']['buckets']))
                out.write("\n")
                if "after_key" not in res['aggregations']['two']:
                    break

                after_dict = {"urls":res['aggregations']['two']['after_key']['urls']}
                query['aggs']['two']["composite"]["after"] = after_dict
    except Exception as e:
        log.exception()
