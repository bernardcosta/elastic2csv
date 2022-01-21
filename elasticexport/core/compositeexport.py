from elasticsearch import Elasticsearch
import json
import os
from datetime import datetime
import logging
import sys
log = logging.getLogger(__name__)


if __name__ == "__main__":

    logging.basicConfig(filename=f'std.log', level=logging.INFO, filemode='w', format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    log.info(f'Starting export')
    log.info(sys.argv[1])
    log.info(len(sys.argv))
    try:
        es = Elasticsearch([os.environ["PFBISERVER"]])
        log.info("Connected to elasticsearch server")
        log.info(str(sys.argv[1]))
        with open(str(sys.argv[1]), encoding='utf-8') as f:
            query = json.loads(f.read())
            log.info('Reading request file')
        # print(query)
        # query = '{"query": {"match_all": {}}}'
        # key_exists([key for key in get_keys(query)])
        with open(sys.argv[2], 'w') as out:
            while True:
                res = es.search(index=str(os.environ['INDEX']), body=query)
                # print(res)
                # print(r)
                out.write(str(res['aggregations']['two']['buckets']))
                if "after_key" not in res['aggregations']['two']:
                    break

                after_dict = {"urls":res['aggregations']['two']['after_key']['urls']}
                query['aggs']['two']["composite"]["after"] = after_dict
    except Exception as e:
        log.info(e)
