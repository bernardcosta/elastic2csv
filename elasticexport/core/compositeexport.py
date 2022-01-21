from elasticsearch import Elasticsearch
import json
import os
from datetime import datetime
import logging
import sys
log = logging.getLogger(__name__)

# def get_keys(dictionary):
#     for key, value in dictionary.items():
#         yield key
#         if type(value) is dict:
#             yield from get_keys(value)
#
#
# def key_exists(keys):
#     if 'composite' not in keys:
#         raise KeyError('"Composite" key must be in query.')


if __name__ == "__main__":

    logging.basicConfig(filename=f'std.log', level=logging.DEBUG, filemode='w', format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    log.debug(f'Starting export')
    log.debug(sys.argv[1])
    log.debug(len(sys.argv))
    try:
        es = Elasticsearch([f'{os.environ["LOCALHOST"]}:5601'])
        log.debug("Connected to elasticsearch server")
        with open(str(sys.argv[1]), encoding='utf-8') as f:
            query = json.loads(f.read())

        # key_exists([key for key in get_keys(query)])
        while True:
            res = es.search(index=os.environ['INDEX'], body=query)
            print(res['aggregations']['two']['buckets'])
            if "after_key" not in res['aggregations']['two']:
                break

            after_dict = {"urls":res['aggregations']['two']['after_key']['urls']}
            query['aggs']['two']["composite"]["after"] = after_dict
    except Exception as e:
        log.error(e)
