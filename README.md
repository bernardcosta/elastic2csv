# elastic2csv

This python repo. downloads data from an `elasticsearch` server using the `composite` query to be able to handle large amounts of data efficiently. The data dump is then converted to a csv file using `jq`.

The query to `elasticsearch` should be saved into a file called `request.json` and place in the root python directory `elastic2csv/elastic2csv/`. The file should include the [composite aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html) query like below:

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

 The name of the elasticsearch index, the elasticsearch server hostname and port are passed as environment variables. Ex:

```
 ESSERVER=127.0.0.1:9200
 INDEX=myindex-*
```
## Run with provided Docker Container

If using the provided docker container, then the above environment variables should be placed in a `.env` file in the parent repo directory i.e. same directory as `docker-compose.yml`.
1. Place the above environment variables in a `.env` file in the same directory as `docker-compose.yml`
2. Replace the `INDEX` and `ESSEERVER` values to the you are using
3. Save your desired query, as detailed in the first section, to `request.json` in the python directory `/elastic2csv/elastic2csv/`

4. `run.sh` runs the docker container and the conversion from json response to CSV using `jq` with the below line:
```
cat elasticexport/tmp_dump.json | jq -r '.[] | [.key.urls,.doc_count,.three.value] | @csv' > output.csv
```

5. Change `.key.urls,.doc_count,.three.value` to any field you want to keep in the CSV file. Each element in the above separated by a comma will be a column in the CSV file. For example if the elasticsearch output is the below:
```
[{
  "key": {
  "urls": "http://example.com"},
 "doc_count": 45841
}]
```
Then the jq command can look like
```
cat elasticexport/tmp_dump.json | jq -r '.[] | [.key.urls,.doc_count,.three.value] | @csv' > output.csv
```
or any other combination of fields you like.

6. Run `run.sh` from terminal

## Running without Docker

To run without docker install the necessary python requirements:
1. `cd` into the python directory and run:
```
pip install -r requirements.txt
```
2. Make sure you have `INDEX` and `ESSERVER` as environment variables pointing to the index and elasticsearch server you are querying from respectively.
3. Follow point 3. to point 6. above (with `run-dockerless.sh` instead of `run.sh`)
