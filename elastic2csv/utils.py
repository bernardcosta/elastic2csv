import collections

def flatten(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_json_list(list):
    flattened_list = []
    for d in list:
        flattened_list.append(flatten(d))

    return flattened_list


def find_key(d):
    for k,v in d.items():
        if isinstance(v, dict):
            p = find_key(v)
            if k == 'composite':
                return [k]
            else:
                return [k] + p
