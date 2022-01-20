from elasticsearch import Elasticsearch
import json
import os
from datetime import datetime
import logging
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
    logging.basicConfig(filename=f'std.log', level=logging.INFO, filemode='w', format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    log.debug(f'Starting export')
    try:
        es = Elasticsearch([os.environ['BISERVER']])
        log.debug("Connected to elasticsearch server")
        with open('new_request.json', encoding='utf-8') as f:
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
