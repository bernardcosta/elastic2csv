class Elastic2csv

def __init__(self):
    pass


def connect(self):
    es = elasticsearch.Elasticsearch(self.opts.url, timeout=CONNECTION_TIMEOUT, http_auth=self.opts.auth,
                                     verify_certs=self.opts.verify_certs, ca_certs=self.opts.ca_certs,
                                     client_cert=self.opts.client_cert, client_key=self.opts.client_key)
    es.cluster.health()
    self.es_conn = es
