# elastic2csv

### Exporting large amounts of data from elasticsearch server to csv file

## Usage

```
main.py [-h] -r REQUEST_FILE -i INDEX [-u URL] [-su SERVER_USERNAME]
               [-sh SERVER_HOST] [-o OUT_DIR]

optional arguments:
  -h, --help            show this help message and exit
  -u URL, --url URL     Host server url with default: http://localhost:9200.
  -su SERVER_USERNAME, --server-username SERVER_USERNAME
                        if url is from remote ssh enter username. You need ssh
                        access credentials for this to work
  -sh SERVER_HOST, --server-host SERVER_HOST
                        if url is from remote ssh enter host. You need ssh
                        access creddential for this to work
  -o OUT_DIR, --out-dir OUT_DIR
                        Directory where to save the json file dump

required named arguments:
  -r REQUEST_FILE, --request-file REQUEST_FILE
                        Json file containing query.
  -i INDEX, --index INDEX
                        Index pattern name example "logstash". "-*" will be
                        added.

```

This python repo. downloads data from an `elasticsearch` server using the `composite` query to be able to handle large amounts of data efficiently.

The file should include the [composite aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html) query like below:

```
  "composite": {
        "size": 700,
        "sources": [
          {
            "foo": {
              "terms": {
                "field": "foo"
              }
            }
          }
        ]
      }
```

 This acts as pagination on the field in order to get all the data efficiently. The above example paginates 700 `elasticsearch` hits at a time and continues retrieving the next set of 700 hits.

 The name of the elasticsearch index, the elasticsearch server hostname and port are passed as environment variables.
