from elasticsearch import Elasticsearch
import json
import os


def get_keys(dictionary):
    for key, value in dictionary.items():
        yield key
        if type(value) is dict:
            yield from get_keys(value)


def key_exists(keys):
    if 'composite' not in keys:
        raise KeyError('"Composite" key must be in query.')


if __name__ == "__main__":
    es = Elasticsearch([os.environ['BISERVER']])

    with open('request.json', encoding='utf-8') as f:
        query = json.loads(f.read())

    key_exists([key for key in get_keys(query)])

    full_buckets = []

    while True:
        res = es.search(index=os.environ['INDEX'], body=query)
        full_buckets.extend(res['aggregations']['two']['buckets'])
        if "after_key" not in res['aggregations']['two']:
            break
        print(res['aggregations']['two']['after_key']['urls'])
        after_dict = {"urls":res['aggregations']['two']['after_key']['urls']}
        query['aggs']['two']["composite"]["after"] = after_dict

    with open('results.json', 'w') as fout:
        json.dump(full_buckets, fout)
